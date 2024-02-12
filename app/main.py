import argparse
import asyncio
import os
from asyncio import IncompleteReadError, StreamReader, StreamWriter

from app.commands import (
    handle_config_get,
    handle_echo,
    handle_get,
    handle_info,
    handle_list_keys,
    handle_ping,
    handle_set,
    init_rdb_parser,
)
from app.expiry import actively_expire_keys
from app.resp import RESPReader, RESPWriter
from app.replication import replication_handshake

role = "master"
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

    kv_store = init_rdb_parser(rdb_parser_required, rdb_file_path)
    global DATASTORE
    DATASTORE |= kv_store
    # Union the parsed kv-store from the rdb file with our internal DATSTORE

    while not stream_reader.at_eof():
        try:
            msg = await reader.read_message()
            print(msg)
        except (IncompleteReadError, ConnectionResetError) as err:
            print(err)
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
            case "KEYS":
                await handle_list_keys(writer, msg, DATASTORE)
            case "INFO":
                await handle_info(writer, msg, role)
            case _:
                print(f"Unknown command received : {command}")
                return


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dir", type=str, help="The directory where RDB files are stored"
    )
    parser.add_argument("--dbfilename", type=str, help="The name of the RDB file")
    parser.add_argument(
        "--port", type=str, help="The port to which this instance will bind"
    )
    parser.add_argument("--replicaof", nargs=2, help="Specify the host and port")

    args = parser.parse_args()

    if args.dir and args.dbfilename:
        global rdb_file_path, rdb_parser_required
        CONFIG["dir"] = dir = str(args.dir)
        CONFIG["dbfilename"] = filename = str(args.dbfilename)
        rdb_parser_required = True
        rdb_file_path = os.path.join(dir, filename)
    else:
        rdb_file_path = ""
        rdb_parser_required = False

    host, port = "127.0.0.1", 6379
    if args.port:
        port = int(args.port)

    if args.replicaof:
        global role
        role = "slave"
        master_host, master_port = args.replicaof
        reader, writer = await asyncio.open_connection(master_host, master_port)
        asyncio.create_task(replication_handshake(reader, writer))

    server = await asyncio.start_server(handler, host, port, reuse_port=False)
    print(f"Started Redis server @ {host}:{port}")

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
