import os
import random
import string

from app.rdb import RDBParser


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
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(length))
