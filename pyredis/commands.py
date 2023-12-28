from pyredis.types import BulkString, Error, SimpleString, Integer, Array


def _handle_echo(command, datastore):
    if len(command) == 2:
        message = command[1].data.decode()
        return BulkString(f'{message}')
    return Error("ERR wrong number of arguments for 'echo' command")


def _handle_ping(command, datastore):
    if len(command) > 1:
        message = command[1].data.decode()
        return BulkString(f'{message}')
    return SimpleString('PONG')


def _handle_unrecognised_command(command):
    args = ' '.join((f"'{c.data.decode()}'" for c in command[1:]))
    return Error(
        f"ERR unknown command '{command[0].data.decode()}', with args beginning with: {args}"
    )


def _handle_get(command, datastore):
    if len(command) == 2:
        key = command[1].data.decode()
        try:
            value = datastore[key]
        except KeyError:
            return BulkString(None)
        return BulkString(value)
    return Error("ERR wrong number of arguments for 'get' command")


def _handle_set(command, datastore):
    length = len(command)

    if length >= 3:
        key = command[1].data.decode()
        value = command[2].data.decode()

        if length == 3:
            datastore[key] = value
            return SimpleString('OK')
        elif length == 5:
            expiry_mode = command[3].data.decode()
            try:
                expiry = int(command[4].data.decode())
            except ValueError:
                return Error('ERR value is not an integer or out of range')

            if expiry_mode == 'ex':
                datastore.set_with_expiry(key, value, expiry)
                return SimpleString('OK')
            elif expiry_mode == 'px':
                datastore.set_with_expiry(key, value, expiry / 1000)
                return SimpleString('OK')
        return Error('ERR syntax error')
    return Error("Error wrong number of arguments for 'set' command")


def _handle_exists(command, datastore):
    if len(command) >= 2:
        count = 0
        for c in command[1:]:
            key = c.data.decode()
            if key in datastore:
                count += 1
        return Integer(count)
    return Error("ERR wrong number of arguments for 'exists' command")


def _handle_del(command, datastore):
    if len(command) > 2:
        count = 0
        for c in command[1:]:
            key = c.data.decode()
            if key in datastore:
                del datastore[key]
                count += 1
        return Integer(count)
    return Error("ERR wrong number of arguments for 'del' command")


def _handle_incr(command, datastore):
    if len(command) == 2:
        key = command[1].data.decode()
        try:
            return Integer(datastore.incr(key))
        except:
            return Error("ERR value is not an integer or out of range")
    return Error("ERR wrong number of arguments for 'incr' command")


def _handle_decr(command, datastore):
    if len(command) == 2:
        key = command[1].data.decode()
        try:
            return Integer(datastore.decr(key))
        except:
            return Error("ERR value is not an integer or out of range")
    return Error("ERR wrong number of arguments for 'decr' command")


def _handle_lpush(command, datastore):
    if len(command) >= 2:
        count = 0
        key = command[1].data.decode()

        try:
            for c in command[2:]:
                item = c.data.decode()
                count = datastore.prepend(key, item)
            return Integer(count)
        except TypeError:
            return Error("WRONGTYPE Operation against a key holding the wrong kind of value")
    return Error("ERR wrong number of arguments for 'lpush' command")


def _handle_rpush(command, datastore):
    if len(command) >= 2:
        count = 0
        key = command[1].data.decode()

        try:
            for c in command[2:]:
                item = c.data.decode()
                count = datastore.append(key, item)
            return Integer(count)
        except TypeError:
            return Error("WRONGTYPE Operation against a key holding the wrong kind of value")
    return Error("ERR wrong number of arguments for 'rpush' command")


def _handle_lrange(command, datastore):
    if len(command) == 4:
        key = command[1].data.decode()
        start = int(command[2].data.decode())
        stop = int(command[3].data.decode())

        try:
            items = datastore.lrange(key, start, stop)
            return Array([BulkString(i) for i in items])
        except TypeError:
            return Error("WRONGTYPE Operation against a key holding the wrong kind of value")

    return Error("ERR wrong number of arguments for 'lrange' command")


def handle_command(command, datastore):
    match command[0].data.decode().upper():
        case "ECHO":
            return _handle_echo(command, datastore)
        case "PING":
            return _handle_ping(command, datastore)
        case "GET":
            return _handle_get(command, datastore)
        case "SET":
            return _handle_set(command, datastore)
        case "DECR":
            return _handle_decr(command, datastore)
        case "DEL":
            return _handle_del(command, datastore)
        case "EXISTS":
            return _handle_exists(command, datastore)
        case "INCR":
            return _handle_incr(command, datastore)
        case "LPUSH":
            return _handle_lpush(command, datastore)
        case "LRANGE":
            return _handle_lrange(command, datastore)
        case "RPUSH":
            return _handle_rpush(command, datastore)

    return _handle_unrecognised_command(command)
