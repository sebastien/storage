import asyncio
import json
import shutil
import tempfile
import unittest
from uuid import uuid4

from storage import DirectoryBackend, JournalBackend, MemoryBackend, Types
from storage.objects import ObjectStorage, StoredObject
from storage.raw import RawStorage, StoredRaw
from storage.web import StorageServer, http

from extra.handler import AWSLambdaEvent
from extra.http.model import HTTPBodyAsyncStream, HTTPBodyBlob, HTTPBodyFile, HTTPResponse
from extra.model import Application


@http("items")
class WebItem(StoredObject):
	PROPERTIES = dict(value=Types.STRING)
	RELATIONS = lambda self: dict(tags=[WebTag])

	@http("rename", methods="POST")
	def rename(self, value):
		self.value = value
		self.save()
		return {"value": self.value}

	@http("describe", methods="GET")
	def describe(self, prefix=""):
		return {"value": prefix + self.value}


@http("tags")
class WebTag(StoredObject):
	PROPERTIES = dict(label=Types.STRING)


@http("blobs")
class WebBlob(StoredRaw):
	pass


class StorageWebTest(unittest.TestCase):
	def setUp(self):
		self.path = tempfile.mkdtemp(prefix="storage-web-")
		self.objects = ObjectStorage(DirectoryBackend(self.path)).use(WebItem, WebTag)
		self.raw = RawStorage(DirectoryBackend(self.path)).use(WebBlob)
		self.server = StorageServer(prefix="/api", classes=(WebItem, WebTag, WebBlob))
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

		response, payload = self.request("GET", f"/api/items/{id}?strict")
		self.assertEqual(response.status, 404)
		self.assertEqual(payload.decode("utf8"), "Not Found")

	def testNonStrictGetDoesNotCreateMissingObject(self):
		missingId = f"missing-{uuid4().hex}"
		response, fetched = self.requestJSON("GET", f"/api/items/{missingId}")
		self.assertEqual(response.status, 200)
		self.assertEqual(fetched["id"], missingId)
		self.assertFalse(WebItem.Has(missingId))

		response, payload = self.request("GET", f"/api/items/{missingId}?strict")
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

	def testRelationReadAndMutations(self):
		tags = [WebTag(label=_).save() for _ in ("a", "b", "c", "d")]
		item = WebItem(value="alpha", tags=tags[:2]).save()

		response, relations = self.requestJSON("GET", f"/api/items/{item.id}/relations")
		self.assertEqual(response.status, 200)
		self.assertEqual(2, relations["relations"]["tags"]["count"])

		response, count = self.requestJSON("GET", f"/api/items/{item.id}/relations/tags/count")
		self.assertEqual(response.status, 200)
		self.assertEqual(2, count["count"])

		response, page = self.requestJSON("GET", f"/api/items/{item.id}/relations/tags/list/0:1")
		self.assertEqual(response.status, 200)
		self.assertEqual(1, page["count"])
		self.assertEqual(tags[0].id, page["values"][0]["id"])
		self.assertNotIn("label", page["values"][0])

		response, resolved = self.requestJSON("GET", f"/api/items/{item.id}/relations/tags/list/0:1?resolve=1")
		self.assertEqual(response.status, 200)
		self.assertEqual("a", resolved["values"][0]["label"])

		response, page = self.requestJSON(
			"POST",
			f"/api/items/{item.id}/relations/tags/append",
			body=json.dumps({"values": [{"id": tags[2].id, "type": "tags"}]}),
			headers={"Content-Type": "application/json"},
		)
		self.assertEqual(response.status, 200)
		self.assertEqual(3, page["total"])
		self.assertEqual([_.id for _ in tags[:3]], [_.id for _ in WebItem.Get(item.id).tags])

		response, page = self.requestJSON(
			"POST",
			f"/api/items/{item.id}/relations/tags/insert",
			body=json.dumps({"index": 1, "values": [{"id": tags[3].id, "type": "tags"}]}),
			headers={"Content-Type": "application/json"},
		)
		self.assertEqual(response.status, 200)
		self.assertEqual([tags[0].id, tags[3].id, tags[1].id, tags[2].id], [_.id for _ in WebItem.Get(item.id).tags])

		response, page = self.requestJSON(
			"POST",
			f"/api/items/{item.id}/relations/tags/swap",
			body=json.dumps({"a": 0, "b": 2}),
			headers={"Content-Type": "application/json"},
		)
		self.assertEqual(response.status, 200)
		self.assertEqual([tags[1].id, tags[3].id, tags[0].id, tags[2].id], [_.id for _ in WebItem.Get(item.id).tags])

		response, page = self.requestJSON(
			"POST",
			f"/api/items/{item.id}/relations/tags/move",
			body=json.dumps({"from": 1, "to": 4}),
			headers={"Content-Type": "application/json"},
		)
		self.assertEqual(response.status, 200)
		self.assertEqual([tags[1].id, tags[0].id, tags[2].id, tags[3].id], [_.id for _ in WebItem.Get(item.id).tags])

		response, page = self.requestJSON(
			"POST",
			f"/api/items/{item.id}/relations/tags/remove",
			body=json.dumps({"values": [{"id": tags[0].id, "type": "tags"}]}),
			headers={"Content-Type": "application/json"},
		)
		self.assertEqual(response.status, 200)
		self.assertEqual([tags[1].id, tags[2].id, tags[3].id], [_.id for _ in WebItem.Get(item.id).tags])

		response, page = self.requestJSON(
			"POST",
			f"/api/items/{item.id}/relations/tags/delete",
			body=json.dumps({"index": 1}),
			headers={"Content-Type": "application/json"},
		)
		self.assertEqual(response.status, 200)
		self.assertEqual([tags[1].id, tags[3].id], [_.id for _ in WebItem.Get(item.id).tags])

		response, page = self.requestJSON(
			"POST",
			f"/api/items/{item.id}/relations/tags/clear",
			body=json.dumps({}),
			headers={"Content-Type": "application/json"},
		)
		self.assertEqual(response.status, 200)
		self.assertEqual([], list(WebItem.Get(item.id).tags))

	def testUpdateRejectsEmptyFieldNameWithStructuredError(self):
		item = WebItem(value="alpha").save()
		response, error = self.requestJSON(
			"POST",
			f"/api/items/{item.id}",
			body=json.dumps({"": "beta"}),
			headers={"Content-Type": "application/json"},
		)
		self.assertEqual(response.status, 400)
		self.assertEqual(error["errno"], "EMPTYKEY")
		self.assertEqual(error["status"], 400)
		self.assertIn("problem", error)
		self.assertIn("expected", error)
		self.assertEqual(error["received"]["field"], "")
		self.assertEqual(error["context"]["path"], f"/api/items/{item.id}")

	def testUpdateRejectsNonObjectPayloadWithStructuredError(self):
		item = WebItem(value="alpha").save()
		response, error = self.requestJSON(
			"POST",
			f"/api/items/{item.id}",
			body=json.dumps(["beta"]),
			headers={"Content-Type": "application/json"},
		)
		self.assertEqual(response.status, 400)
		self.assertEqual(error["errno"], "BADPAYLOAD")
		self.assertIn("Expected object payload", error["problem"])
		self.assertIn("expected", error)

	def testCommandsRejectMalformedPayloadWithStructuredError(self):
		response, error = self.requestJSON(
			"POST",
			"/api/commands",
			body=json.dumps({"commands": {"op": "update"}}),
			headers={"Content-Type": "application/json"},
		)
		self.assertEqual(response.status, 400)
		self.assertEqual(error["errno"], "BADLIST")
		self.assertIn("commands", error["problem"])

	def testUpdateCommandRejectsEmptyFieldNameWithStructuredError(self):
		item = WebItem(value="alpha").save()
		response, payload = self.requestJSON(
			"POST",
			"/api/commands",
			body=json.dumps(
				{
					"commands": [
						{
							"op": "update",
							"type": "items",
							"id": item.id,
							"fields": {"": "beta"},
						}
					]
				}
			),
			headers={"Content-Type": "application/json"},
		)
		self.assertEqual(response.status, 200)
		result = payload["results"][0]
		self.assertFalse(result["ok"])
		self.assertEqual(result["errno"], "EMPTYKEY")
		self.assertEqual(result["status"], 400)
		self.assertEqual(result["received"]["field"], "")
		self.assertEqual(result["context"]["index"], 0)

	def testChannelSSEFromJournal(self):
		self.objects.release()
		self.objects = ObjectStorage(JournalBackend(MemoryBackend())).use(WebItem)
		item = WebItem(value="alpha").save()
		response, channel = self.requestJSON("POST", "/api/channel")
		self.assertEqual(response.status, 200)

		async def run():
			async def post_json(path, data):
				request = AWSLambdaEvent.AsRequest(
					AWSLambdaEvent.Create(
						"POST",
						path,
						body=json.dumps(data),
						headers={"Content-Type": "application/json"},
					)
				)
				response = self.app.process(request)
				if not isinstance(response, HTTPResponse):
					response = await response
				return response

			request = AWSLambdaEvent.AsRequest(
				AWSLambdaEvent.Create("GET", f"/api/channel/{channel['id']}/events")
			)
			response = self.app.process(request)
			if not isinstance(response, HTTPResponse):
				response = await response
			self.assertEqual(response.status, 200)
			self.assertIsInstance(response.body, HTTPBodyAsyncStream)
			stream = response.body.stream
			try:
				ready = await anext(stream)
				self.assertIn("event: ready", ready)
				await post_json(
					f"/api/channel/{channel['id']}/commands",
					{
						"commands": [
							{
								"op": "subscribe",
								"target": {
									"kind": "object",
									"type": "items",
									"id": item.id,
								},
							}
						]
					},
				)
				item.value = "beta"
				item.save()
				update = await asyncio.wait_for(anext(stream), timeout=1)
				self.assertTrue("event: update" in update or "event: create" in update)
				self.assertIn('"value":"beta"', update)
			finally:
				await stream.aclose()

		asyncio.run(run())
		response, closed = self.requestJSON("POST", f"/api/channel/{channel['id']}/close")
		self.assertEqual(response.status, 200)
		self.assertTrue(closed["ok"])


if __name__ == "__main__":
	unittest.main()

# EOF
