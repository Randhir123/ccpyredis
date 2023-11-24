from pyredis.types import Array, BulkString, Error, Integer, SimpleString

_MSG_SEPARATOR = b"\r\n"
_MSG_SEPARATOR_SIZE = len(_MSG_SEPARATOR)


def extract_frame_from_buffer(buffer):
    # print(f"Bufer in Extract: {buffer}")
    separator = buffer.find(_MSG_SEPARATOR)

    if separator == -1:
        return None, 0
    else:
        payload = buffer[1:separator].decode()

        match chr(buffer[0]):
            case "+":
                return SimpleString(payload), separator + _MSG_SEPARATOR_SIZE

            case "-":
                return Error(payload), separator + _MSG_SEPARATOR_SIZE

            case ":":
                return Integer(int(payload)), separator + _MSG_SEPARATOR_SIZE

            case "$":
                length = int(payload)

                if length == -1:
                    return None, 5
                else:
                    if (
                            len(buffer)
                            < separator + _MSG_SEPARATOR_SIZE + length + _MSG_SEPARATOR_SIZE
                    ):
                        return None, 0
                    else:
                        end_of_message = separator + 2 + length
                        return (
                            BulkString(buffer[separator + 2: end_of_message]),
                            end_of_message + _MSG_SEPARATOR_SIZE,
                        )

            case "*":
                length = int(payload)

                if length == 0:
                    return Array([]), separator + _MSG_SEPARATOR_SIZE

                if length == -1:
                    return Array(None), separator + _MSG_SEPARATOR_SIZE

                array = []

                for _ in range(length):
                    next_item, length = extract_frame_from_buffer(
                        buffer[separator + _MSG_SEPARATOR_SIZE:]
                    )

                    if next_item and length:
                        array.append(next_item)
                        separator += length
                    else:
                        return None, 0

                return Array(array), separator + _MSG_SEPARATOR_SIZE
    return None, 0


def encode_message(message):
    return message.resp_encode()

