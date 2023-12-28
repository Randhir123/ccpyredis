from collections import deque
from time import time_ns, sleep

import pytest

from pyredis.datastore import Datastore, to_ns


@pytest.fixture
def ds():
    return Datastore()


def test_intial_data_invalid_type():
    with pytest.raises(TypeError):
        ds = Datastore("string")


def test_initial_data():
    ds = Datastore({"k1": 1, "k2": "v2"})
    assert ds["k1"] == 1
    assert ds["k2"] == "v2"


def test_in(ds):
    with pytest.raises(KeyError):
        ds["key"] = 1
        assert "key" in ds
        assert "key2" not in ds


def test_get_item(ds):
    ds["key"] = 1
    assert ds["key"] == 1


def test_set_item(ds):
    l = ds.append("key", 1)
    assert l == 1
    assert ds["key"] == deque([1])


def test_incr(ds):
    ds["k"] = "1"
    res = ds.incr("k")
    assert res == 2
    res = ds.incr("k")
    assert res == 3


def test_decr(ds):
    ds["k"] = "1"
    ds.incr("k")
    ds.incr("k")
    res = ds.incr("k")
    assert res == 4
    res = ds.decr("k")
    assert res == 3
    res = ds.decr("k")
    assert res == 2


def test_append(ds):
    num_entries = ds.append("key", 1)
    assert num_entries == 1
    assert ds["key"] == deque([1])


def test_prepend(ds):
    ds.append("key", 1)
    ds.prepend("key", 2)
    assert ds["key"] == deque([2, 1])


def test_set_with_expiry(ds):
    expiry = 1
    ds.set_with_expiry("key", "value", expiry)
    expected = time_ns() + to_ns(expiry)

    assert ds["key"] == "value"
    diff = expected - ds._data["key"].expiry
    assert diff < 10000


def test_expire_on_read(ds):
    ds.set_with_expiry("key", "value", 0.01)
    sleep(0.15)
    with pytest.raises(KeyError):
        ds["key"]


def test_remove_expired_keys_empty():
    ds = Datastore()
    ds.remove_expired_keys()


def _fill_ds(ds, size, percent_expired):
    num_expired = int(size * (percent_expired / 100))

    # items without expiry
    for i in range(size - num_expired):
        ds[f"{i}"] = i

    # items with expiry and that will have expired
    for i in range(num_expired):
        ds.set_with_expiry(f"e_{i}", i, -1)


@pytest.mark.parametrize("size, percent_expired", [
    (20, 10),
    (200, 100)
])
def test_remove_expired_keys(size, percent_expired):
    expected_len_after_expiry = size - (size * (percent_expired / 100))

    ds = Datastore()
    _fill_ds(ds, size, percent_expired)

    ds.remove_expired_keys()
    assert len(ds._data) == expected_len_after_expiry
