from app.expiry import (EXPIRY_TIMESTAMP_DEFAULT_VAL, check_key_expiration,
                        get_expiry_timestamp)
from app.resp import RESPWriter


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
