import asyncio
import time

EXPIRY_TIMESTAMP_DEFAULT_VAL = 0


async def actively_expire_keys(
    DATASTORE: dict[str, tuple[str, int]], ACTIVE_KEY_EXPIRY_TIME_WINDOW: int
):
    """
    This async function iterates through all keys in a dictionary and expires
    them if their expiry time is older than the current timestamp.
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
    current_timestamp = int(time.time() * 1000)  # ms
    if expiry_timestamp == EXPIRY_TIMESTAMP_DEFAULT_VAL:
        return False
    elif current_timestamp > expiry_timestamp:
        del DATASTORE[key]
        return True
    return False


def get_expiry_timestamp(msg: list[str]) -> int:
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
