import asyncio
import time

EXPIRY_TIMESTAMP_DEFAULT_VAL = 0


async def actively_expire_keys(
    DATASTORE: dict[str, tuple[str, int]], ACTIVE_KEY_EXPIRY_TIME_WINDOW: int
):
    """
    Periodically scans the provided data store (dictionary) for expired keys and
    removes them.
    This asynchronous function runs in an infinite loop, performing the
    following actions at each iteration:
    * Gets the current timestamp.
    * Iterates through all keys in the data store.
    * For each key, checks if its expiry timestamp is older than the current
      timestamp (meaning it has expired).
    * If a key is expired, it's removed from the data store.
    * Waits for the specified `ACTIVE_KEY_EXPIRY_TIME_WINDOW` before repeating
      the process.

    This function ensures that keys with invalid expiry times are automatically
    removed when they become stale, maintaining efficient memory usage in the
    data store.
    """
    while True:
        current_time = time.time()
        expired_keys: list[str] = []
        for key, (_, expiry_timestamp) in DATASTORE.items():
            if current_time > expiry_timestamp:
                expired_keys.append(key)

        for key in expired_keys:
            del DATASTORE[key]

        await asyncio.sleep(ACTIVE_KEY_EXPIRY_TIME_WINDOW)


def check_key_expiration(
    DATASTORE: dict[str, tuple[str, int]], key: str, expiry_timestamp: int
) -> bool:
    """
    Checks if a specific key in the datastore has expired based on its expiry
    timestamp. This function compares the current timestamp with the provided
    `expiry_timestamp` stored for the key:

    * If the `expiry_timestamp` is the default value (0), it signifies the key
      never expires and `False` is returned.
    * If the current timestamp is greater than the `expiry_timestamp`, the key
      is considered expired and, it is deleted from the `DATASTORE`. `True` is
      returned to indicate the expiration.
    * Otherwise, the key is not expired and `False` is returned.
    """
    current_timestamp = int(time.time() * 1000)  # ms
    if expiry_timestamp == EXPIRY_TIMESTAMP_DEFAULT_VAL:
        return False
    elif current_timestamp > expiry_timestamp:
        del DATASTORE[key]
        return True
    return False


def get_expiry_timestamp(msg: list[str]) -> int:
    """
    Parses the provided RESP command message and extracts the optional key
    expiry timestamp.
    This function checks if the message includes an expiry argument ("EX" or
    "PX") and the corresponding numerical value. If found, it converts the value
    to milliseconds and adds it to the current timestamp to define the absolute
    expiry time. Otherwise, it returns the default value indicating no active
    expiry.
    This function is used when setting keys to determine their valid lifespan
    based on the provided command options.
    """
    if len(msg) > 3:
        opt, expiry = msg[3], msg[4]
        expiry = int(expiry)
        if opt == "ex":  # seconds
            expiry = expiry * 1000
        current_timestamp = int(time.time() * 1000)  # ms
        expiry_timestamp = current_timestamp + expiry
    else:
        expiry_timestamp = EXPIRY_TIMESTAMP_DEFAULT_VAL

    return expiry_timestamp
