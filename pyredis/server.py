import socket
import threading

from pyredis.commands import handle_command
from pyredis.datastore import Datastore
from pyredis.protocol import extract_frame_from_buffer, encode_message

RECV_SIZE = 2048


def handle_client_connection(client_socket, datastore):
    buffer = bytearray()

    try:
        while True:
            data = client_socket.recv(RECV_SIZE)

            if not data:
                break

            buffer.extend(data)

            frame, frame_size = extract_frame_from_buffer(buffer)

            if frame:
                buffer = buffer[frame_size:]
                result = handle_command(frame, datastore)
                client_socket.send(encode_message(result))
    finally:
        client_socket.close()


class Server:
    def __init__(self, port):
        self.port = port
        self._running = False
        self._datastore = Datastore()

    def run(self):
        self._running = True

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_address = ('localhost', self.port)
            server_socket.bind(server_address)
            server_socket.listen()

            while self._running:
                connection, _ = server_socket.accept()

                client_handler = threading.Thread(
                    target=handle_client_connection, args=(connection, self._datastore)
                )
                client_handler.start()

    def stop(self):
        self._running = False
