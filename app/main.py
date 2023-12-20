import asyncio
from asyncio import IncompleteReadError, StreamReader, StreamWriter
import time

from app.resp import RESPReader, RESPWriter
from app.active import actively_expire_keys

HOST, PORT = "127.0.0.1", 6379
DATASTORE: dict[str, tuple[str, int]] = {}  # key -> (value, expiry_timestamp)


async def handler(stream_reader: StreamReader, stream_writer: StreamWriter):
    reader, writer = RESPReader(stream_reader), RESPWriter(stream_writer)
    while 1:
        try:
            msg = await reader.read_message()
        except IncompleteReadError:
            return
        print(msg)
        command = msg[0].upper()
        match command:
            case "PING":
                response = "PONG"
                await writer.write_simple_string(response)
            case "ECHO":
                response = msg[1]
                await writer.write_bulk_string(response)
            case "SET":
                if len(msg) > 3:
                    opt, expiry = msg[3], msg[4]
                    expiry = int(expiry)
                    if opt == "ex":  # seconds
                        expiry = expiry * 1000
                    current_timestamp = int(time.time() * 1000)  # ms
                    expiry_timestamp = current_timestamp + expiry
                else:
                    expiry_timestamp = 0
                key, value = msg[1], msg[2]
                DATASTORE[key] = (value, expiry_timestamp)
                await writer.write_simple_string("OK")
            case "GET":
                key = msg[1]
                default_value = None
                val = DATASTORE.get(key, default_value)
                if val is not None:
                    value, expiry_timestamp = val
                    current_timestamp = int(time.time() * 1000)  # ms
                    if expiry_timestamp == 0:
                        response = value
                    elif current_timestamp > expiry_timestamp:
                        del DATASTORE[key]
                        response = None
                    else:
                        response = value
                else:
                    response = None
                await writer.write_bulk_string(response)
            case _:
                raise RuntimeError(f"Unknown command received : {command}")


async def main():
    server = await asyncio.start_server(handler, HOST, PORT, reuse_port=False)
    print(f"Started Redis server @ {HOST}:{PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
        asyncio.create_task(actively_expire_keys(DATASTORE))
    except KeyboardInterrupt:
        print("Interrupted, shutting down.")
