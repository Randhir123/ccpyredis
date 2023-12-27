import asyncio
import threading
from time import sleep

import typer

from pyredis.asyncserver import RedisServerProtocol
from pyredis.datastore import Datastore
from pyredis.server import Server

REDIS_DEFAULT_PORT = 6379


# def main(port=None):
#     if port is None:
#         port = REDIS_DEFAULT_PORT
#     else:
#         port = int(port)
#
#     print(f"Starting PyRedis on port: {port}")
#
#     server = Server(port)
#     server.run()
#
#
# if __name__ == '__main__':
#     typer.run(main)


def check_expiry(datastore):
    while True:
        datastore.remove_expired_keys()
        sleep(0.1)


async def check_expiry_task(datastore):
    while True:
        datastore.remove_expired_keys()
        await asyncio.sleep(0.1)


async def main(port=None):
# def main(port=None):
    if port is None:
        port = REDIS_DEFAULT_PORT
    else:
        port = int(port)

    print(f"Starting PyRedis on port: {port}")

    datastore = Datastore()

    loop = asyncio.get_running_loop()

    task = loop.create_task(check_expiry_task(datastore))

    server = await loop.create_server(
        lambda: RedisServerProtocol(datastore), "127.0.0.1", port
    )

    async with server:
        await server.serve_forever()

    # expiration_monitor = threading.Thread(target=check_expiry, args=(datastore,))
    # expiration_monitor.start()
    #
    # server = Server(port)
    # server.run()


if __name__ == '__main__':
    typer.run(asyncio.run(main()))
    # typer.run(main)
