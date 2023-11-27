import asyncio

import typer

from pyredis.asyncserver import RedisServerProtocol
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

async def main(port=None):
    if port is None:
        port = REDIS_DEFAULT_PORT
    else:
        port = int(port)

    print(f"Starting PyRedis on port: {port}")

    loop = asyncio.get_running_loop()

    server = await loop.create_server(
        lambda: RedisServerProtocol(), "127.0.0.1", port
    )

    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    typer.run(asyncio.run(main()))
