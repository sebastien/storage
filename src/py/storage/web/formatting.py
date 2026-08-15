"""Pure response formatting helpers used by the web server."""

import json
from html import escape


def formatJSON(value):
	return json.dumps(value)


def formatMarkdown(value):
	lines = _markdownLines(value)
	return "\n".join(lines or ["- null"]) + "\n"


def _markdownLines(value, indent=0, name=None):
	prefix = " " * indent
	if isinstance(value, dict):
		lines = []
		if name is not None:
			lines.append("%s- %s:" % (prefix, name))
		for key, item in value.items():
			lines.extend(_markdownLines(item, indent + (2 if name is not None else 0), str(key)))
		if lines:
			return lines
		if name is not None:
			return ["%s- %s: {}" % (prefix, name)]
		return ["%s- {}" % prefix]
	elif isinstance(value, list):
		lines = []
		if name is not None:
			lines.append("%s- %s:" % (prefix, name))
			indent += 2
			prefix = " " * indent
		for item in value:
			if isinstance(item, (dict, list)):
				lines.append("%s-" % prefix)
				lines.extend(_markdownLines(item, indent + 2))
			else:
				lines.append("%s- %s" % (prefix, _markdownScalar(item)))
		if lines:
			return lines
		if name is not None:
			return ["%s- %s: []" % (" " * (indent - 2), name)]
		return ["%s- []" % prefix]
	text = _markdownScalar(value)
	if name is None:
		return ["%s- %s" % (prefix, text)]
	return ["%s- %s: %s" % (prefix, name, text)]


def _markdownScalar(value):
	if value is None:
		return "null"
	elif value is True:
		return "true"
	elif value is False:
		return "false"
	return str(value)


def formatXML(value):
	body = _xmlValue(value, "response")
	return '<?xml version="1.0" encoding="utf-8"?>\n%s\n' % body


def _xmlValue(value, name):
	tag = _xmlTag(name)
	if isinstance(value, dict):
		if not value:
			return "<%s/>" % tag
		children = [_xmlValue(item, str(key)) for key, item in value.items()]
		return "<%s>%s</%s>" % (tag, "".join(children), tag)
	elif isinstance(value, list):
		if not value:
			return "<%s/>" % tag
		children = "".join(_xmlValue(item, "item") for item in value)
		return "<%s>%s</%s>" % (tag, children, tag)
	return "<%s>%s</%s>" % (tag, escape(_xmlScalar(value)), tag)


def _xmlScalar(value):
	if value is None:
		return ""
	elif value is True:
		return "true"
	elif value is False:
		return "false"
	return str(value)


def _xmlTag(name):
	text = str(name or "item")
	chars = []
	for i, char in enumerate(text):
		if char.isalnum() or char in ("_", "-", "."):
			if i == 0 and char.isdigit():
				chars.append("n")
			chars.append(char)
		else:
			chars.append("-")
	return "".join(chars) or "item"


def formatSSE(event, data=None, id=None):
	lines = []
	if event:
		lines.append("event: %s" % event)
	if id is not None:
		lines.append("id: %s" % id)
	text = json.dumps(data or {}, separators=(",", ":"))
	for line in text.splitlines() or [""]:
		lines.append("data: %s" % line)
	return "\n".join(lines) + "\n\n"


def describeValue(value):
	if isinstance(value, dict):
		return dict(type="object", keys=list(value.keys()))
	elif isinstance(value, list):
		return dict(type="list", length=len(value))
	return dict(type=type(value).__name__, value=value)


def requestFormat(request):
	path = getattr(request, "path", "") or ""
	if path.endswith(".md"):
		return "md"
	elif path.endswith(".xml"):
		return "xml"
	return None


def stripFormatSuffix(value, format):
	if format == "md" and isinstance(value, str) and value.endswith(".md"):
		return value[:-3]
	elif format == "xml" and isinstance(value, str) and value.endswith(".xml"):
		return value[:-4]
	return value


class StorageFormatting:
	"""Delegating methods retained so server subclasses remain customizable."""

	def formatJSON(self, value):
		return formatJSON(value)

	def formatMarkdown(self, value):
		return formatMarkdown(value)

	def formatXML(self, value):
		return formatXML(value)

	def formatSSE(self, event, data=None, id=None):
		return formatSSE(event, data=data, id=id)

	def describeValue(self, value):
		return describeValue(value)

	def requestFormat(self, request):
		return requestFormat(request)

	def stripFormatSuffix(self, value, format):
		return stripFormatSuffix(value, format)


__all__ = [
	"StorageFormatting",
	"describeValue",
	"formatJSON",
	"formatMarkdown",
	"formatSSE",
	"formatXML",
	"requestFormat",
	"stripFormatSuffix",
]


def __getattr__(name):
	# Keep the staged compatibility import lazy so this module never imports the server eagerly.
	if name == "StorageServer":
		from .server import StorageServer

		return StorageServer
	raise AttributeError(name)
