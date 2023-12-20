import argparse
import asyncio
from asyncio import IncompleteReadError, StreamReader, StreamWriter

from app.commands import (handle_config_get, handle_echo, handle_get,
                          handle_ping, handle_set)
from app.expiry import actively_expire_keys
from app.resp import RESPReader, RESPWriter

HOST, PORT = "127.0.0.1", 6379
ACTIVE_KEY_EXPIRY_TIME_WINDOW = 60  # seconds
DATASTORE: dict[str, tuple[str, int]] = {}  # key -> (value, expiry_timestamp)
# Main Datastore, All SET, GET data is stored in this global dict.
CONFIG: dict[str, str] = {}  # key -> value (Redis config parameters)


async def handler(stream_reader: StreamReader, stream_writer: StreamWriter):
    """
    Handles incoming RESP commands from Redis clients and interacts with the
    application logic. This asynchronous function continuously performs the
    following tasks in a loop:
    * Reads a single RESP message from the `stream_reader` using the
      `RESPReader`.
    * Handles potential read errors by closing the connection and returning.
    * Extracts the first element of the parsed message as the command name
      (converted to uppercase).
    * Matches the command name to known handlers, and executes the handler
      function to perform the required operation.
    """
    reader, writer = RESPReader(stream_reader), RESPWriter(stream_writer)
    while not stream_reader.at_eof():
        try:
            msg = await reader.read_message()
        except (IncompleteReadError, ConnectionResetError):
            await writer.close()
            return
        command = msg[0].upper()
        match command:
            case "PING":
                await handle_ping(writer)
            case "ECHO":
                await handle_echo(writer, msg)
            case "SET":
                await handle_set(writer, msg, DATASTORE)
            case "GET":
                await handle_get(writer, msg, DATASTORE)
            case "CONFIG":
                await handle_config_get(writer, msg, CONFIG)
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
