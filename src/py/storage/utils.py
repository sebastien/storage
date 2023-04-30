from typing import Any
from contextlib import contextmanager
from threading import Lock
from math import floor


TPrimitive = dict[str, Any] | list[Any] | str | int | float | bool


@contextmanager
def atomic(lock: Lock):
    lock.acquire()
    try:
        yield None
    except Exception as e:
        raise e
    finally:
        lock.release()


CHARS: str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def strhash(text: str, seed: int = 5381) -> int:
    """Returns a hash for the given text"""
    hash_value = 5381
    for char in text:
        hash_value = (hash_value * 33) ^ ord(char)
    return hash_value & 0xFFFFFFFF


def numcode(num: int, alphabet: str = CHARS) -> str:
    """Formats a number into a string using the given alphabet."""
    res: list[str] = []
    n: int = len(alphabet)
    v: int = abs(num)
    while v > 0:
        res.insert(0, alphabet[v % n])
        v = floor(v / n)
    return "".join(res)


# EOF
