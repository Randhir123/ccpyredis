from pyredis.types import BulkString, Error, SimpleString


def _handle_echo(command):
    if len(command) == 2:
        message = command[1].data.decode()
        return BulkString(f'{message}')
    return Error("ERR wrong number of arguments for 'echo' command")


def _handle_ping(command):
    if len(command) > 1:
        message = command[1].data.decode()
        return BulkString(f'{message}')
    return SimpleString('PONG')


def _handle_unrecognised_command(command):
    args = ' '.join((f"'{c.data.decode()}'" for c in command[1:]))
    return Error(
        f"ERR unknown command '{command[0].data.decode()}', with args beginning with: {args}"
    )


def handle_command(command):
    match command[0].data.decode().upper():
        case "ECHO":
            return _handle_echo(command)
        case "PING":
            return _handle_ping(command)

    return _handle_unrecognised_command(command)
