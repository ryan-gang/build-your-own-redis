from app.expiry import (
    EXPIRY_TIMESTAMP_DEFAULT_VAL,
    check_key_expiration,
    get_expiry_timestamp,
)
from app.resp import RESPWriter


async def handle_ping(writer: RESPWriter):
    response = "PONG"
    await writer.write_simple_string(response)


async def handle_echo(writer: RESPWriter, msg: list[str]):
    response = msg[1]
    await writer.write_bulk_string(response)


async def handle_set(
    writer: RESPWriter, msg: list[str], DATASTORE: dict[str, tuple[str, int]]
):
    key, value = msg[1], msg[2]
    DATASTORE[key] = (value, get_expiry_timestamp(msg))
    await writer.write_simple_string("OK")


async def handle_get(
    writer: RESPWriter, msg: list[str], DATASTORE: dict[str, tuple[str, int]]
):
    key = msg[1]
    default_value = (None, EXPIRY_TIMESTAMP_DEFAULT_VAL)
    value, expiry_timestamp = DATASTORE.get(key, default_value)
    expired = check_key_expiration(DATASTORE, key, expiry_timestamp)
    if expired:
        value = None
    await writer.write_bulk_string(value)


async def handle_config_get(writer: RESPWriter, msg: list[str], CONFIG: dict[str, str]):
    key = msg[2]
    value = CONFIG.get(key, None)
    await writer.write_array([key, value])
