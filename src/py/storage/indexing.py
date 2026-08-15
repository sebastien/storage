"""Pure value extraction helpers used by the index runtime."""

import re
import unicodedata
from typing import Optional

from .core import Storable


RE_SPACES = re.compile(r"[\s\t\n]+")
RE_NOALPHANUM = re.compile("[^A-Za-z0-9]+")


class Indexing:
	"""A collection of functions that transform the given value into a value
	or a list of values to be used with indexing."""

	@classmethod
	def Value(cls, value, object=None):
		"""Transparent, just returns the value."""
		return value

	@classmethod
	def Normalize(cls, value: str, object=None) -> str:
		"""Converts the word to UTF-8, lowercase, stripped and with single spaces."""
		return RE_SPACES.sub(" ", str(value or "").lower()).strip()

	# SEE: http://stackoverflow.com/questions/517923/what-is-the-best-way-to-remove-accents-in-a-python-unicode-string
	@classmethod
	def NoAccents(cls, value, object=None):
		value = str(value) if type(value) is not str else value
		nkfd_form = unicodedata.normalize("NFKD", value)
		return str(nkfd_form.encode("ASCII", "ignore"), "ascii")

	@classmethod
	def UpdateTime(cls, value, object: Storable = None) -> Optional[int]:
		"""Returns the update time of the given object"""
		return object.getUpdateTime() if object else None

	@classmethod
	def Keyword(cls, value, object=None) -> str:
		"""Normalizes the given value as a keyword. It will be filtered
		through `NoAccents`, then non-alphanumeric characters will be replaced
		by spaces, and the result  will be normalized."""
		text = str(value, "utf8") if not isinstance(value, str) else value
		text = cls.NoAccents(text)
		text = RE_NOALPHANUM.sub(" ", text)
		text = cls.Normalize(text)
		return text

	@classmethod
	def Keywords(cls, values, object=None, minLength=3):
		"""Extracts keywords from the the given object. Returns them
		noramlized and without accents."""
		res = set()
		if isinstance(values, dict):
			value = list(values.values())
		if type(values) not in (tuple, list):
			values = (values,)
		for value in values:
			if not value:
				continue
			# We might have i18n fields that are like {en:XXX,fr:XXX}
			if isinstance(value, dict):
				words = []
				for _ in list(value.values()):
					words.extend(_.split(" "))
			else:
				words = value.split(" ")
			for word in words:
				word = cls.Keyword(word)
				if word and len(word) >= minLength:
					res.add(word)
		return list(res)

	@classmethod
	def Properties(cls, **properties):
		"""Returns a function that will index the given `properties` using
		the given extractor. (for instance `Properties(name=Normalize)`)"""

		def indexer(cls, values, object=None):
			res = []
			scope = values
			for name, extractor in list(properties.items()):
				v = getattr(scope, name)
				v = extractor(v, scope)
				if type(v) in (tuple, list):
					res.extend(v)
				else:
					res.append(v)
			return res

		return indexer

	@classmethod
	def Paths(cls, separator):
		def f(value, object=None, sep=separator):
			res = []
			current = None
			if not value:
				return res
			for _ in value.split(sep):
				if not _:
					continue
				if current is None:
					current = _
				else:
					current += sep + _
				res.append(current)
			return res

		return f


# Keep persisted names stable while allowing the implementation to live here.
Indexing.__module__ = "storage.index"


__all__ = ["RE_SPACES", "RE_NOALPHANUM", "Indexing"]
