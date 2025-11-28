from typing import ClassVar
from datetime import datetime


class MapType:
	def __init__(self, kwargs):
		self.kwargs = kwargs

	def __str__(self):
		return "{%s}" % (",".join(["%s=%s" % _ for _ in list(self.kwargs.items())]))

	def __call__(self, **kwargs):
		res = {}
		for key, value in list(kwargs.items()):
			assert key in self.kwargs, "Key not part of type: %s not in %s" % (
				key,
				self,
			)
			# TODO: Should also validate type
			res[key] = value
		return res


class Types:
	BOOL: ClassVar[str] = "bool"
	DATE: ClassVar[str] = "date"
	TIME: ClassVar[str] = "time"
	DATETIME: ClassVar[str] = "datetime"
	NUMBER: ClassVar[str] = "number"
	INTEGER: ClassVar[str] = "int"
	POSITIVE: ClassVar[str] = "int>0"
	FLOAT: ClassVar[str] = "float"
	STRING: ClassVar[str] = "string"
	LINE: ClassVar[str] = "string:line"
	RICHTEXT: ClassVar[str] = "string:richtext"
	MARKDOWN: ClassVar[str] = "string:markdown"
	I18NSTRING: ClassVar[str] = "string:i18n"
	EMAIL: ClassVar[str] = "string:email"
	PASSWORD: ClassVar[str] = "string:password"
	URL: ClassVar[str] = "string:url"
	PATH: ClassVar[str] = "string:path"
	ID: ClassVar[str] = "string:id"
	HTML: ClassVar[str] = "string:html"
	BINARY: ClassVar[str] = "bin"
	ANY: ClassVar[str] = "any"
	THIS: ClassVar[str] = "¤"

	@staticmethod
	def RANGE(start, end, type=INTEGER):
		return "%s:%s-%s" % (type, start, end)

	@staticmethod
	def LIST(_=ANY):
		return "[%s]" % (_)

	@staticmethod
	def TUPLE(*_):
		return "(%s)" % (",".join(map(str, _)))

	@staticmethod
	def ONE_OF(*_):
		return "(%s)" % ("|".join(map(str, _)))

	@staticmethod
	def MAP(**kwargs):
		return MapType(kwargs)

	@staticmethod
	def ENUM(*args):
		return "(%s)" % ("|".join(["%s" % _ for _ in args]))

	@staticmethod
	def REFERENCE(clss):
		return clss.__name__

	@classmethod
	def AsString(cls, value):
		if isinstance(value, str) or isinstance(value, str):
			return value
		else:
			return str(value)

	@classmethod
	def AsBoolean(cls, value):
		if (
			isinstance(value, str)
			or isinstance(value, str)
			and value.lower() == "false"
		):
			return False
		return value and True or False

	@classmethod
	def AsDate(cls, value):
		"""We expect a string of format yyyy-mm-dd we don't do any validation
		We sipmly conver it to a list if it isn't"""
		if isinstance(value, datetime):
			return list(value.timetuple())
		elif isinstance(value, list) or isinstance(value, tuple):
			return value
		# We expect the date to be formatted as YYYY-MM-DD
		try:
			date = datetime.strptime(value, "%Y-%m-%d")
			date = list(date.timetuple())
		except:
			date = None
		return date


# EOF
