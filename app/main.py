import asyncio
from asyncio import StreamReader, StreamWriter

HOST, PORT = "localhost", 6379


async def handler(reader: StreamReader, writer: StreamWriter):
    while 1:
        data = await reader.read(1024)
        if not data:
            break
        writer.write("+PONG\r\n".encode())
        await writer.drain()
    writer.write_eof()
    writer.close()


async def main():
    server = await asyncio.start_server(handler, HOST, PORT, reuse_port=True)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted, shutting down.")
