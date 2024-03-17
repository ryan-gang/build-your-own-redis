import asyncio
import binascii
import bisect
import time
from typing import Any

from app.expiry import (EXPIRY_TIMESTAMP_DEFAULT_VAL, check_key_expiration,
                        get_expiry_timestamp)
from app.resp import RESPReader, RESPWriter
from app.util import generate_random_string

stream_entries = dict[str, str]
stream = dict[str, stream_entries]
streams: dict[str, stream] = {}  # stream_key -> stream


type_mapping = {
    "str": "string",
    "int": "integer",
    "float": "float",
    "list": "list",
    "dict": "hash",
    "set": "set",
    "bool": "boolean",
    "NoneType": "none",
    "stream_key_type": "stream",
}


class stream_key_type:
    def __init__(self, val: str):
        val = val


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


async def handle_type(
    writer: RESPWriter, msg: list[str], datastore: dict[str, tuple[Any, int]]
):
    """
    Handles the TYPE command from the Redis client.
    This function extracts the message to echo from the second element of the
    `msg` list and writes it back to the client using the `write_bulk_string`
    method.
    """
    key = msg[1]
    value, _ = datastore.get(key, (None, None))
    if value is None:
        type_name = "none"
    else:
        type_name = (type(value)).__name__
    response = type_mapping.get(type_name, "none")
    await writer.write_simple_string(response)


async def handle_set(
    writer: RESPWriter, msg: list[str], DATASTORE: dict[str, tuple[str, int]]
):
    """
    Handles the SET command from the Redis client.

    This function performs the following actions:
    * Extracts the key and value from the `msg` list.
    * Checks if an optional TTL (time-to-live) is provided, else sets a default
      placeholder.
    * Updates the `DATASTORE` dictionary with the new key-value pair and expiry
      timestamp.
    * Writes the "OK" response to the client using the `write_simple_string`
      method.
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
        * If expired, passively removes the key from the `DATASTORE` and
          returns a null bulk string to the client.
    * If the key doesn't exist, returns a null bulk string to the client.
    """
    key = msg[1]
    default_value = (None, EXPIRY_TIMESTAMP_DEFAULT_VAL)
    value, expiry_timestamp = DATASTORE.get(key, default_value)
    expired = check_key_expiration(DATASTORE, key, expiry_timestamp)
    if expired:
        value = None
    await writer.write_bulk_string(value)


async def handle_config_get(
    writer: RESPWriter, msg: list[str], CONFIG: dict[str, str]
):
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

    This function retrieves the list of all keys from the DATASTORE and sends
    it back to the client as an RESP array.
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
        response += "\r\nmaster_repl_offset:0"

    await writer.write_bulk_string(response)


async def handle_replconf(writer: RESPWriter, msg: list[str]):
    """
    Handles the REPLCONF command from the Redis client.
    """
    response = "OK"
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
    hex_str = (
        "524544495330303131fa0972656469732d76657205372e322e30"
        "fa0a72656469732d62697473c040fa056374696d65c26d08bc65"
        "fa08757365642d6d656dc2b0c41000fa08616f662d62617365c0"
        "00fff06e3bfec0ff5aa2"
    )

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
    msg: list[str],
):
    """
    Handles the WAIT command from the Redis client.
    """
    updated_replicas = 0
    start_time = time.time()

    await asyncio.sleep(0.125)
    num_replicas, timeout = int(msg[1]), int(msg[2])

    if master_offset == 0:
        response = len(replicas)
    else:
        for repl_reader, repl_writer in replicas:
            await repl_writer.write_array(["REPLCONF", "GETACK", "*"])

        for repl_reader, repl_writer in replicas:
            try:
                response = await asyncio.wait_for(
                    repl_reader.read_array(skip_first_byte=True), timeout=0.125
                )
                print("response", response)
                if response[0] == "REPLCONF" and response[1] == "ACK":
                    repl_offset = int(response[2])
                    if repl_offset >= master_offset:
                        updated_replicas += 1
            except asyncio.TimeoutError:
                print("Timeout expired. No data received.")

        response = updated_replicas

    end_time = time.time()
    elapsed_time = (end_time - start_time) * 1000

    if response < num_replicas and master_offset != 0:
        t = max(0, timeout - elapsed_time)
        print(f"Waiting for {t} ms.")
        await asyncio.sleep(t / 1000)
    await writer.write_integer(response)
    return


async def handle_xadd(
    writer: RESPWriter, msg: list[str], datastore: dict[str, tuple[Any, int]]
):
    stream_key = msg[1]
    stream_entry_id = msg[2]
    stream_entry_list = msg[3:]

    stream_entry = {}
    for i in range(0, len(stream_entry_list), 2):
        stream_entry[stream_entry_list[i]] = stream_entry_list[i + 1]
    stream = streams.get(stream_key, {})

    if stream_entry_id == "*":
        current_timestamp = int(time.time() * 1000)
        current_sequence = 0
    else:
        current_timestamp, current_sequence = stream_entry_id.split("-")
        current_timestamp = int(current_timestamp)

    if len(stream) > 0:
        stream_last_entry_id = sorted(list(stream.keys()))[-1]
        stream_last_entry = stream[stream_last_entry_id]
    else:
        stream_last_entry = None

    if current_sequence == "*":
        if stream_last_entry is None:
            if current_timestamp != 0:
                current_sequence = 0
            else:
                current_sequence = 1
        else:
            stream_last_entry_id = sorted(list(stream.keys()))[-1]
            stream_last_entry_timestamp, stream_last_entry_sequence = (
                stream_last_entry_id.split("-")
            )
            stream_last_entry_timestamp = int(stream_last_entry_timestamp)
            stream_last_entry_sequence = int(stream_last_entry_sequence)
            if current_timestamp == stream_last_entry_timestamp:
                current_sequence = stream_last_entry_sequence + 1
            else:
                if current_timestamp != 0:
                    current_sequence = 0
                else:
                    current_sequence = 1
    else:
        current_sequence = int(current_sequence)

    stream_entry_id = f"{current_timestamp}-{current_sequence}"

    if current_timestamp == 0 and current_sequence == 0:
        await writer.write_simple_error(
            "ERR The ID specified in XADD must be greater than 0-0"
        )
        return
    if stream_last_entry is not None:
        stream_last_entry_timestamp, stream_last_entry_sequence = (
            stream_last_entry_id.split("-")
        )
        stream_last_entry_timestamp = int(stream_last_entry_timestamp)
        stream_last_entry_sequence = int(stream_last_entry_sequence)

        if current_timestamp < stream_last_entry_timestamp or (
            current_timestamp <= stream_last_entry_timestamp
            and current_sequence <= stream_last_entry_sequence
        ):
            await writer.write_simple_error(
                "ERR The ID specified in XADD is equal or smaller than the"
                " target stream top item"
            )
            return

    # Add stream_key to Datastore as a new object stream()
    stream[stream_entry_id] = stream_entry
    streams[stream_key] = stream

    datastore[stream_key] = (
        stream_key_type(stream_key),
        get_expiry_timestamp([]),
    )  # No expiry
    # Add stream_key to datastore, to handle TYPE on it.

    response = stream_entry_id
    await writer.write_bulk_string(response)


async def handle_xrange(writer: RESPWriter, msg: list[str]):
    stream_key = msg[1]
    stream_entry_id_start = msg[2]
    stream_entry_id_end = msg[3]

    stream = streams.get(stream_key, [])  # Handle case where it is empty
    stream_keys = sorted(list(stream.keys()))

    if stream_entry_id_start == "-":
        start_idx = 0
    else:
        start_idx = bisect.bisect_left(stream_keys, stream_entry_id_start)

    if stream_entry_id_end == "+":
        end_idx = len(stream_keys) - 1
    else:
        end_idx = bisect.bisect_left(stream_keys, stream_entry_id_end)

    output = _format_fetched_stream_entries_for_xrange(
        _fetch_stream_entries(stream, stream_keys, start_idx, end_idx)
    )
    await writer.write_array(output)


async def handle_xread(writer: RESPWriter, msg: list[str]):
    stream_key = msg[2]
    stream_entry_id_start = msg[3]

    stream = streams.get(stream_key, [])  # Handle case where it is empty
    stream_keys = sorted(list(stream.keys()))

    start_idx = bisect.bisect_left(stream_keys, stream_entry_id_start)
    end_idx = len(stream_keys) - 1

    entries = _fetch_stream_entries(stream, stream_keys, start_idx, end_idx)
    output = _format_fetched_stream_entries_for_xread(entries, stream_key)

    await writer.write_array([output])


def _fetch_stream_entries(
    stream: dict[str, stream_entries],
    stream_keys: list[str],
    start_idx: int,
    end_idx: int,
) -> list[tuple[str, stream_entries]]:
    entries: list[tuple[str, stream_entries]] = []
    for key in stream_keys[start_idx : end_idx + 1]:
        entries.append((key, stream[key]))
    return entries


def _format_fetched_stream_entries_for_xrange(
    entries: list[tuple[str, stream_entries]],
) -> list[str]:
    inner_list_type = list[tuple[str, list[str]]]
    output: list[inner_list_type] = []
    for entry in entries:
        inner: inner_list_type = []
        key = entry[0]
        inner.append(key)
        d = entry[1]
        d_as_list: list[str] = []
        for k, v in d.items():
            d_as_list.append(k)
            d_as_list.append(v)
        inner.append(d_as_list)
        output.append(inner)
    return output


def _format_fetched_stream_entries_for_xread(
    entries: list[tuple[str, stream_entries]], stream_key: str
) -> list[str]:
    inner_list_type = list[tuple[str, list[str]]]
    output: list[inner_list_type] = []
    for entry in entries:
        inner: inner_list_type = []
        key = entry[0]
        inner.append(key)
        d = entry[1]
        d_as_list: list[str] = []
        for k, v in d.items():
            d_as_list.append(k)
            d_as_list.append(v)
        inner.append(d_as_list)
        output.append(inner)

    return [stream_key, output]
