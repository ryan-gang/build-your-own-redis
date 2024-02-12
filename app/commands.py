import os
import random
import string
import binascii

from app.expiry import (
    EXPIRY_TIMESTAMP_DEFAULT_VAL,
    check_key_expiration,
    get_expiry_timestamp,
)
from app.rdb import RDBParser
from app.resp import RESPReader, RESPWriter
import asyncio

async def handle_ping(writer: RESPWriter):
    """
    Handles the PING command from the Redis client.

    This function simply writes the "PONG" response to the `writer` using the
    `write_simple_string` method.
    """
    response = "PONG"
    await writer.write_simple_string(response)


async def handle_echo(writer: RESPWriter, msg: list[str]):
    """
    Handles the ECHO command from the Redis client.

    This function extracts the message to echo from the second element of the
    `msg` list and writes it back to the client using the `write_bulk_string`
    method.
    """
    response = msg[1]
    await writer.write_bulk_string(response)


async def handle_set(
    writer: RESPWriter, msg: list[str], DATASTORE: dict[str, tuple[str, int]]
):
    """
    Handles the SET command from the Redis client.

    This function performs the following actions:
    * Extracts the key and value from the `msg` list.
    * Checks if an optional TTL (time-to-live) is provided, else sets a default placeholder.
    * Updates the `DATASTORE` dictionary with the new key-value pair and expiry timestamp.
    * Writes the "OK" response to the client using the `write_simple_string` method.
    """
    key, value = msg[1], msg[2]
    DATASTORE[key] = (value, get_expiry_timestamp(msg))
    await writer.write_simple_string("OK")


async def handle_get(
    writer: RESPWriter, msg: list[str], DATASTORE: dict[str, tuple[str, int]]
):
    """
    Handles the GET command from the Redis client.

    This function performs the following actions:
    * Retrieves the key from the `msg` list.
    * Checks if the key exists in the `DATASTORE` dictionary.
    * If the key exists:
        * Checks if the key has expired using the `check_key_expiration`
          function.
        * If not expired, returns the value to the client using the
          `write_bulk_string` method.
        * If expired, passively removes the key from the `DATASTORE` and returns
          a null bulk string to the client.
    * If the key doesn't exist, returns a null bulk string to the client.
    """
    key = msg[1]
    default_value = (None, EXPIRY_TIMESTAMP_DEFAULT_VAL)
    value, expiry_timestamp = DATASTORE.get(key, default_value)
    expired = check_key_expiration(DATASTORE, key, expiry_timestamp)
    if expired:
        value = None
    await writer.write_bulk_string(value)


async def handle_config_get(writer: RESPWriter, msg: list[str], CONFIG: dict[str, str]):
    """
    Handles the CONFIG GET command from the Redis client.

    This function retrieves the requested configuration value from the `CONFIG`
    dictionary and sends it back to the client as an RESP array.
    """
    key = msg[2]
    value = CONFIG.get(key, None)
    await writer.write_array([key, value])


async def handle_list_keys(
    writer: RESPWriter, msg: list[str], DATASTORE: dict[str, tuple[str, int]]
):
    """
    Handles the KEYS * command from the Redis client.

    This function retrieves the list of all keys from the DATASTORE and sends it
    back to the client as an RESP array.
    """
    key = msg[1]
    assert key == "*"
    key_list = list(DATASTORE.keys())
    await writer.write_array(key_list)


async def handle_info(writer: RESPWriter, msg: list[str], role: str):
    """
    Handles the INFO command from the Redis client.
    """
    header = f"# {msg[1]}.capitalize()"
    response = f"{header}\r\nrole:{role}"
    if role == "master":
        response += f"\r\nmaster_replid:{generate_random_string(40)}"
        response += f"\r\nmaster_repl_offset:0"

    await writer.write_bulk_string(response)


async def handle_replconf(writer: RESPWriter, msg: list[str]):
    """
    Handles the REPLCONF command from the Redis client.
    """
    response = "OK"
    if msg[1] == "ACK":
        print("Received ACK in Master thread.")
    await writer.write_simple_string(response)


async def handle_psync(writer: RESPWriter, msg: list[str]):
    """
    Handles the REPLCONF command from the Redis client.
    """
    response = f"FULLRESYNC {generate_random_string(40)} 0"
    await writer.write_simple_string(response)


async def handle_rdb_transfer(writer: RESPWriter, msg: list[str]):
    """
    Handles the REPLCONF command from the Redis client.
    """
    hex_str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

    try:
        # Decode the hex string to bytes
        bytes_data = binascii.unhexlify(hex_str)
    except binascii.Error as e:
        print(f"Encountered {e} while decoding hex string")
        return None  # Adjust based on how you want to handle errors

    resp = b"$" + str(len(bytes_data)).encode() + b"\r\n"
    message = resp + bytes_data

    await writer.write_raw(message)


async def handle_wait(
    writer: RESPWriter,
    replicas: list[tuple[RESPReader, RESPWriter]],
    master_offset: int,
):
    """
    Handles the WAIT command from the Redis client.
    """
    updated_replicas = 0
    await asyncio.sleep(1)

    if master_offset > 0:
        for repl_reader, repl_writer in replicas:
            await repl_writer.write_array(["REPLCONF", "GETACK", "*"])
            # response = await repl_reader.read_array()
            try:
                response = await asyncio.wait_for(repl_reader.read_array(skip_first_byte=True), timeout=1.0)
                print("response", response)
                if response[0] == "REPLCONF" and response[1] == "ACK":
                    repl_offset = response[2]
                    if repl_offset >= master_offset:
                        updated_replicas += 1
            except asyncio.TimeoutError:
                print('Timeout expired. No data received.')

        await writer.write_integer(updated_replicas)
    else:
        await writer.write_integer(len(replicas))
    return

def init_rdb_parser(
    parsing_reqd_flag: bool, rdb_file_path: str
) -> dict[str, tuple[str, int]]:
    """
    Simple utility function that only parses the .rdb file if parsing_reqd_flag
    is set, and if the file exists. Returns the parsed key-value store, or an
    empty dict.
    """
    if parsing_reqd_flag and os.path.isfile(rdb_file_path):
        parser = RDBParser(rdb_file_path)
        return parser.kv
    return {}


def generate_random_string(length: int) -> str:
    letters_and_digits = string.ascii_lowercase + string.digits
    result_str = "".join(random.choice(letters_and_digits) for _ in range(length))
    return result_str
