from time import sleep, time_ns, time

import pytest

from pyredis.commands import handle_command
from pyredis.datastore import Datastore
from pyredis.types import Array, BulkString, Error, SimpleString


@pytest.mark.parametrize(
    "command, expected",
    [
        # Echo Tests
        (
                Array([BulkString(b"ECHO")]),
                Error("ERR wrong number of arguments for 'echo' command"),
        ),
        (Array([BulkString(b"echo"), BulkString(b"Hello")]), BulkString("Hello")),
        (
                Array([BulkString(b"echo"), BulkString(b"Hello"), BulkString("World")]),
                Error("ERR wrong number of arguments for 'echo' command"),
        ),
        # Ping Tests
        (Array([BulkString(b"ping")]), SimpleString("PONG")),
        (Array([BulkString(b"ping"), BulkString(b"Hello")]), BulkString("Hello")),

        # Set with Expire Errors
        (
                Array([BulkString(b"set"), SimpleString(b"key"), SimpleString(b"value"), SimpleString(b"ex")]),
                Error("ERR syntax error"),
        ),
        (
                Array([BulkString(b"set"), SimpleString(b"key"), SimpleString(b"value"), SimpleString(b"px")]),
                Error("ERR syntax error"),
        ),
        (
                Array([BulkString(b"set"), SimpleString(b"key"), SimpleString(b"value"), SimpleString(b"foo")]),
                Error("ERR syntax error"),
        ),
    ],
)
def test_handle_command(command, expected):
    datastore = Datastore()
    result = handle_command(command, datastore)
    assert result == expected


def test_set_and_get_item():
    ds = Datastore()
    ds['key'] = 1
    assert ds['key'] == 1


def test_expire_on_reads():
    ds = Datastore()
    ds.set_with_expiry("key", "value", 0.01)
    sleep(0.15)
    with pytest.raises(KeyError):
        ds['key']


def test_get_with_expiry():
    datastore = Datastore()
    key = 'key'
    value = 'value'
    px = 100

    command = [
        BulkString(b'set'),
        SimpleString(b'key'),
        SimpleString(b'value'),
        BulkString(b'px'),
        BulkString(f'{px}'.encode())
    ]
    result = handle_command(command, datastore)
    assert result == SimpleString('OK')
    sleep((px + 100) / 1000)
    command = [BulkString(b'get'), SimpleString(b'key')]
    result = handle_command(command, datastore)
    assert result == BulkString(None)


def test_set_with_expiry():
    datastore = Datastore()
    key = 'key'
    value = 'value'
    ex = 1
    px = 100

    base_command = [BulkString(b'set'), SimpleString(b'key'), SimpleString(b'value')]

    # seconds
    command = base_command[:]
    command.extend([BulkString(b'ex'), BulkString(f'{ex}'.encode())])
    expected_expiry = time_ns() + (ex * 10 ** 9)
    result = handle_command(command, datastore)
    assert result == SimpleString('OK')
    stored = datastore._data[key]
    assert stored.value == value
    diff = - expected_expiry - stored.expiry
    assert diff < 10000

    # milliseconds
    command = base_command[:]
    command.extend([BulkString(b'px'), BulkString(f'{px}'.encode())])
    expected_expiry = time_ns() + (px * 10 ** 6)
    result = handle_command(command, datastore)
    assert result == SimpleString('OK')
    stored = datastore._data[key]
    assert stored.value == value
    diff = - expected_expiry - stored.expiry
    assert diff < 10000
