from typing import Any
from contextlib import contextmanager
from threading import Lock


TPrimitive = dict[str, Any]


@contextmanager
def atomic(lock: Lock):
    lock.acquire()
    try:
        yield None
    except Exception as e:
        raise e
    finally:
        lock.release()


# EOF
