"""Structured web API errors."""


class StorageWebError(Exception):
	"""Structured web API error with a compact reportable code."""

	def __init__(self, errno, error, problem, received=None, expected=None, status=400):
		Exception.__init__(self, error)
		self.errno = errno
		self.error = error
		self.problem = problem
		self.received = received
		self.expected = expected
		self.status = status

	def payload(self, context=None):
		result = dict(
			ok=False,
			errno=self.errno,
			error=self.error,
			problem=self.problem,
			expected=self.expected,
			status=self.status,
		)
		if self.received is not None:
			result["received"] = self.received
		if context:
			result["context"] = context
		return result

__all__ = ["StorageWebError"]
