from collections import deque
from dataclasses import dataclass
from itertools import islice
from random import sample
from threading import Lock
from time import time_ns
from typing import Any

from pyredis.types import Error


def to_ns(seconds):
    return seconds * 10 ** 9


@dataclass
class DataEntry:
    """Class to represent a data entry. Contains the data and the expiry."""
    value: Any
    expiry: int = 0


class Datastore:
    """
    The core data store, provides a thread safe dictionary extended with
    the interface needed to support Redis functionality.
    """

    def __init__(self, initial_data=None):
        self._data = dict()
        self._lock = Lock()
        if initial_data:
            if not isinstance(initial_data, dict):
                raise TypeError('Initial Data should be of type dict')

            for key, value in initial_data.items():
                self._data[key] = DataEntry(value)

    def __getitem__(self, key):
        with self._lock:
            item = self._data[key]

            if item.expiry and item.expiry < time_ns():
                del self._data[key]
                raise KeyError

            return item.value

    def __setitem__(self, key, value):
        with self._lock:
            self._data[key] = DataEntry(value)

    def incr(self, key):
        with self._lock:
            item = self._data.get(key, DataEntry(0))
            value = int(item.value) + 1
            item.value = str(value)
            self._data[key] = item
        return value

    def decr(self, key):
        with self._lock:
            value = int(self._data.get(key, DataEntry(0)).value) - 1
            self._data[key].value = str(value)
        return value

    def append(self, key, value):
        with self._lock:
            item = self._data.get(key, DataEntry(deque()))
            if not isinstance(item.value, deque):
                raise TypeError
            item.value.append(value)
            self._data[key] = item
            return len(item.value)

    def lrange(self, key, start, stop):
        with self._lock:
            item = self._data.get(key, DataEntry(deque()))
            if not isinstance(item.value, deque):
                raise TypeError

            return list(islice(item.value, start, stop))

    def prepend(self, key, value):
        with self._lock:
            item = self._data.get(key, DataEntry(deque()))
            print("HERE")
            if not isinstance(item.value, deque):
                print(item.value)
                raise TypeError
            print("HERE 2")
            item.value.insert(0, value)
            self._data[key] = item
            return len(item.value)

    def set_with_expiry(self, key, value, expiry: int):
        with self._lock:
            calculated_expiry = time_ns() + to_ns(expiry)
            self._data[key] = DataEntry(value, calculated_expiry)

    def remove_expired_keys(self):
        while True:
            try:
                keys = sample(list(self._data.keys()), 20)
            except ValueError:
                return

            count_expired = 0
            count_keys = len(keys)

            for key in keys:
                try:
                    with self._lock:
                        item = self._data[key]
                        if item.expiry and item.expiry < int(time_ns()):
                            del self._data[key]
                            count_expired += 1
                except KeyError:
                    pass

            if count_expired / count_keys <= 0.25:
                break
