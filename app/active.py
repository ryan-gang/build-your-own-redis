import asyncio
import time


ACTIVE_KEY_EXPIRY_TIME_WINDOW = 60  # sec


async def actively_expire_keys(DATASTORE: dict[str, tuple[str, int]]):
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
