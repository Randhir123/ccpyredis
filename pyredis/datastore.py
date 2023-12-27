from dataclasses import dataclass
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
