import asyncio

from pyredis.commands import handle_command
from pyredis.datastore import Datastore
from pyredis.protocol import extract_frame_from_buffer, encode_message


class RedisServerProtocol(asyncio.Protocol):
    def __init__(self, datastore):
        self.transport = None
        self.buffer = bytearray()
        self.datastore = datastore

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data: bytes) -> None:
        if not data:
            self.transport.close()

        self.buffer.extend(data)

        frame, frame_size = extract_frame_from_buffer(self.buffer)

        if frame:
            self.buffer = self.buffer[frame_size:]
            result = handle_command(frame, self.datastore)
            self.transport.write(encode_message(result))
