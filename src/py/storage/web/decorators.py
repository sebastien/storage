"""HTTP decoration API."""

import types

from ..core import Storable


class StorageDecoration:
	"""Stores HTTP exposure metadata on storable classes."""

	KEY = "_storage_web_StorageDecoration"
	KEY_FUNCTION = "_storage_web_StorageDecoration_Function"

	@classmethod
	def Get(cls, storableClass):
		return getattr(storableClass, cls.KEY)

	@classmethod
	def Has(cls, storableClass):
		return hasattr(storableClass, cls.KEY)

	def __init__(self, storableClass, url, restrict=None, methods=None, export=None):
		assert issubclass(storableClass, Storable), "Storable class requires a Storable object"
		self.storable = storableClass
		self.url = url
		self.restrict = restrict
		self.httpMethods = [methods] if isinstance(methods, str) else methods
		if isinstance(export, str):
			export = dict(profile=export)
		self.export = export or {}
		self.export["target"] = "web"

	def listInvocables(self, storable=None):
		storable = storable or self.storable
		for name in dir(storable):
			value = getattr(storable, name)
			if hasattr(value, self.KEY_FUNCTION):
				yield (name, getattr(value, self.KEY_FUNCTION))

	def getName(self):
		return self.storable.__name__.split(".")[-1].lower()

	def getExportOptions(self):
		return self.export

	def __repr__(self):
		return "@storage.web:%s(url=%s,storable=%s,methods=%s,restrict=%s)" % (
			self.getName(), self.url, self.storable, self.httpMethods, self.restrict
		)


def http(url, restrict=None, methods=None, contentType=None, export=None):
	"""Adds HTTP exposure information to a storable class or method."""

	def wrapper(storableClassOrFunction):
		if type(storableClassOrFunction) is types.FunctionType:
			setattr(
				storableClassOrFunction,
				StorageDecoration.KEY_FUNCTION,
				(url, restrict, methods, contentType),
			)
		else:
			setattr(
				storableClassOrFunction,
				StorageDecoration.KEY,
				StorageDecoration(storableClassOrFunction, url, restrict, methods, export),
			)
		return storableClassOrFunction

	return wrapper

__all__ = ["StorageDecoration", "http"]
