import asyncio
import json
import shutil
import tempfile
import unittest

from storage import DirectoryBackend, Types
from storage.objects import ObjectStorage, StoredObject
from storage.raw import RawStorage, StoredRaw
from storage.web import StorageServer, http

from extra.handler import AWSLambdaEvent
from extra.http.model import HTTPBodyBlob, HTTPBodyFile, HTTPResponse
from extra.model import Application


@http("items")
class WebItem(StoredObject):
	PROPERTIES = dict(value=Types.STRING)

	@http("rename", methods="POST")
	def rename(self, value):
		self.value = value
		self.save()
		return {"value": self.value}

	@http("describe", methods="GET")
	def describe(self, prefix=""):
		return {"value": prefix + self.value}


@http("blobs")
class WebBlob(StoredRaw):
	pass


class StorageWebTest(unittest.TestCase):
	def setUp(self):
		self.path = tempfile.mkdtemp(prefix="storage-web-")
		self.objects = ObjectStorage(DirectoryBackend(self.path)).use(WebItem)
		self.raw = RawStorage(DirectoryBackend(self.path)).use(WebBlob)
		self.server = StorageServer(prefix="/api", classes=(WebItem, WebBlob))
		self.app = Application()
		self.app.mount(self.server)
		asyncio.run(self.app.start())

	def tearDown(self):
		asyncio.run(self.app.stop())
		self.objects.release()
		self.raw.release()
		shutil.rmtree(self.path)

	def request(self, method, path, body=None, headers=None, app=None):
		app = app or self.app
		event = AWSLambdaEvent.Create(method, path, headers=headers, body=body)
		response = app.process(AWSLambdaEvent.AsRequest(event))
		if not isinstance(response, HTTPResponse):
			response = asyncio.run(response)
		payload = b""
		if isinstance(response.body, HTTPBodyBlob):
			payload = response.body.payload
		elif isinstance(response.body, HTTPBodyFile):
			with open(response.body.path, "rb") as f:
				payload = f.read()
		return response, payload

	def requestJSON(self, method, path, body=None, headers=None, app=None):
		response, payload = self.request(method, path, body=body, headers=headers, app=app)
		data = json.loads(payload.decode("utf8")) if payload else None
		return response, data

	def testCRUDAndInvoke(self):
		response, created = self.requestJSON(
			"POST",
			"/api/items",
			body=json.dumps({"value": "alpha"}),
			headers={"Content-Type": "application/json"},
		)
		self.assertEqual(response.status, 200)
		self.assertEqual(created["value"], "alpha")
		id = created["id"]

		response, fetched = self.requestJSON("GET", f"/api/items/{id}")
		self.assertEqual(response.status, 200)
		self.assertEqual(fetched["id"], id)
		self.assertEqual(fetched["value"], "alpha")

		response, listed = self.requestJSON("GET", "/api/items/list")
		self.assertEqual(response.status, 200)
		self.assertEqual(listed["count"], 1)
		self.assertEqual(listed["values"][0]["id"], id)

		response, updated = self.requestJSON(
			"POST",
			f"/api/items/{id}",
			body="value=beta",
			headers={"Content-Type": "application/x-www-form-urlencoded"},
		)
		self.assertEqual(response.status, 200)
		self.assertEqual(updated["value"], "beta")

		response, renamed = self.requestJSON(
			"POST",
			f"/api/items/{id}/rename",
			body="value=gamma",
			headers={"Content-Type": "application/x-www-form-urlencoded"},
		)
		self.assertEqual(response.status, 200)
		self.assertEqual(renamed["value"], "gamma")

		response, described = self.requestJSON(
			"GET", f"/api/items/{id}/describe?prefix=hi-"
		)
		self.assertEqual(response.status, 200)
		self.assertEqual(described["value"], "hi-gamma")

		response, removed = self.requestJSON("POST", f"/api/items/{id}/remove")
		self.assertEqual(response.status, 200)
		self.assertTrue(removed)

		response, payload = self.request("GET", f"/api/items/{id}?strict=1")
		self.assertEqual(response.status, 404)
		self.assertEqual(payload.decode("utf8"), "Not Found")

	def testReadonly(self):
		readonly = StorageServer(prefix="/api", classes=(WebItem,), readonly=True)
		app = Application()
		app.mount(readonly)
		asyncio.run(app.start())
		try:
			response, payload = self.request(
				"POST",
				"/api/items",
				body=json.dumps({"value": "alpha"}),
				headers={"Content-Type": "application/json"},
				app=app,
			)
			self.assertEqual(response.status, 403)
			self.assertEqual(payload.decode("utf8"), "Unauthorized")
		finally:
			asyncio.run(app.stop())

	def testRawData(self):
		blob = WebBlob(b"payload", contentType="text/plain")
		blob.save()
		response, payload = self.request("GET", f"/api/blobs/{blob.id}/data")
		self.assertEqual(response.status, 200)
		self.assertEqual(payload, b"payload")
		self.assertEqual(response.headers.headers.get("Content-Type"), "text/plain")


if __name__ == "__main__":
	unittest.main()

# EOF
