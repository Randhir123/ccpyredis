import typer

from pyredis.server import Server

REDIS_DEFAULT_PORT = 6379


def main(port=None):
    if port is None:
        port = REDIS_DEFAULT_PORT
    else:
        port = int(port)

    print(f"Starting PyRedis on port: {port}")

    server = Server(port)
    server.run()


if __name__ == '__main__':
    typer.run(main)
