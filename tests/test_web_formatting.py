"""Focused tests for web response formatting."""

import unittest

from storage.web.formatting import (
	StorageFormatting,
	describeValue,
	formatJSON,
	formatMarkdown,
	formatSSE,
	formatXML,
	requestFormat,
	stripFormatSuffix,
)


class Request:
	path = "/items/list.md"


class FormattingTest(unittest.TestCase):
	def test_json_markdown_and_xml(self):
		value = {"name": "A&B", "items": [True, None]}
		self.assertEqual('{"name": "A&B", "items": [true, null]}', formatJSON(value))
		self.assertIn("- name: A&B\n", formatMarkdown(value))
		self.assertIn("<name>A&amp;B</name>", formatXML(value))
		self.assertIn("<items><item>true</item><item></item></items>", formatXML(value))

	def test_sse_and_value_description(self):
		self.assertEqual(
			'event: update\nid: 4\ndata: {"ok":true}\n\n',
			formatSSE("update", {"ok": True}, id=4),
		)
		self.assertEqual({"type": "object", "keys": ["a"]}, describeValue({"a": 1}))
		self.assertEqual({"type": "list", "length": 2}, describeValue([1, 2]))

	def test_format_suffix_helpers(self):
		self.assertEqual("md", requestFormat(Request()))
		self.assertEqual("items", stripFormatSuffix("items.md", "md"))
		self.assertEqual("items.xml", stripFormatSuffix("items.xml", "md"))

	def test_server_methods_delegate_without_server_import(self):
		server = StorageFormatting()
		self.assertEqual(formatSSE("ready"), server.formatSSE("ready"))
		self.assertEqual(formatMarkdown({"ok": True}), server.formatMarkdown({"ok": True}))


if __name__ == "__main__":
	unittest.main()
