import argparse
import asyncio
from asyncio import IncompleteReadError, StreamReader, StreamWriter

from app.expiry import (EXPIRY_TIMESTAMP_DEFAULT_VAL, actively_expire_keys,
                        check_key_expiration, get_expiry_timestamp)
from app.resp import RESPReader, RESPWriter

HOST, PORT = "127.0.0.1", 6379
ACTIVE_KEY_EXPIRY_TIME_WINDOW = 60  # seconds
DATASTORE: dict[str, tuple[str, int]] = {}  # key -> (value, expiry_timestamp)
CONFIG: dict[str, str] = {}


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
                key, value = msg[1], msg[2]
                DATASTORE[key] = (value, get_expiry_timestamp(msg))
                await writer.write_simple_string("OK")
            case "GET":
                key = msg[1]
                default_value = (None, EXPIRY_TIMESTAMP_DEFAULT_VAL)
                value, expiry_timestamp = DATASTORE.get(key, default_value)
                expired = check_key_expiration(DATASTORE, key, expiry_timestamp)
                if expired:
                    value = None
                await writer.write_bulk_string(value)
            case "CONFIG":
                key = msg[2]
                value = CONFIG.get(key, None)
                await writer.write_array([key, value])
            case _:
                raise RuntimeError(f"Unknown command received : {command}")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dir", type=str, help="The directory where RDB files are stored"
    )
    parser.add_argument("--dbfilename", type=str, help="The name of the RDB file")
    args = parser.parse_args()

    if args.dir:
        CONFIG["dir"] = args.dir
    if args.dbfilename:
        CONFIG["dbfilename"] = args.dbfilename

    server = await asyncio.start_server(handler, HOST, PORT, reuse_port=False)
    print(f"Started Redis server @ {HOST}:{PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
        asyncio.create_task(
            actively_expire_keys(DATASTORE, ACTIVE_KEY_EXPIRY_TIME_WINDOW)
        )
    except KeyboardInterrupt:
        print("Interrupted, shutting down.")
