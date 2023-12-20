import asyncio
from asyncio import StreamReader, StreamWriter

from app.resp import RESPReader, RESPWriter

HOST, PORT = "127.0.0.1", 6379


async def handler(stream_reader: StreamReader, stream_writer: StreamWriter):
    reader, writer = RESPReader(stream_reader), RESPWriter(stream_writer)
    while 1:
        msg = await reader.read_message()
        command = msg[0].upper()
        match command:
            case "PING":
                response = "PONG"
                await writer.write_simple_string(response)
            case "ECHO":
                response = msg[1]
                await writer.write_bulk_string(response)
            case _:
                raise RuntimeError(f"Unknown command received : {command}")
        print(msg)


async def main():
    server = await asyncio.start_server(handler, HOST, PORT, reuse_port=False)
    print(f"Started Redis server @ {HOST}:{PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted, shutting down.")
