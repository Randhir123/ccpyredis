import socket

import typer
from typing_extensions import Annotated

from pyredis.protocol import encode_message, extract_frame_from_buffer
from pyredis.types import Array, BulkString

DEFAULT_PORT = 6379
DEFAULT_SERVER = "127.0.0.1"
RECV_SIZE = 1024


def encode_command(command):
    return Array([BulkString(p) for p in command.split()])


def main(
        server: Annotated[str, typer.Argument()] = DEFAULT_SERVER,
        port: Annotated[int, typer.Argument()] = DEFAULT_PORT
):
    with socket.socket() as client_socket:
        client_socket.connect((server, port))

        buffer = bytearray()

        while True:
            command = input(f'{server}:{port}>')

            if command == 'quit':
                break
            else:
                encoded_message = encode_message(encode_command(command))
                client_socket.send(encoded_message)

                while True:
                    # receive response from server
                    data = client_socket.recv(RECV_SIZE)
                    buffer.extend(data)

                    frame, frame_size = extract_frame_from_buffer(buffer)

                    if frame:
                        buffer = buffer[frame_size:]
                        if isinstance(frame, Array):
                            for count, item in enumerate(frame.data):
                                print(f'{count + 1} "{item.as_str()}"')
                        else:
                            print(frame.as_str())
                        break


if __name__ == '__main__':
    typer.run(main)
