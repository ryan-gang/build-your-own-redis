from asyncio import StreamReader
from typing import Any, Optional


class RESPReader(object):
    def __init__(self, reader: StreamReader):
        self.reader = reader

    async def read_message(self) -> Any:
        msg_code = (await self.reader.readexactly(1)).decode()
        match msg_code:
            case "+":  # Simple String
                string = await self.read_simple_string()
                return string
            case "-":  # Simple Error
                err = await self.read_simple_error()
                return err
            case ":":  # Integer
                num = await self.read_integer()
                return num
            case "$":  # Bulk String
                string = await self.read_bulk_string()
                return string
            case "*":  # Array
                arr = await self.read_array()
                return arr
            case _:
                raise RuntimeError(f"Unknown payload identifier : {msg_code}")

    async def read_array(self):
        arr: list[Any] = []

        metadata = await self.read_line()
        if not metadata:
            return arr
        # assert metadata[0] == "*", "Expected * as array payload identifier."
        length = int(metadata)
        if length == -1:
            return None
        for _ in range(length):
            msg = await self.read_message()
            arr.append(msg)
        return arr

    async def read_simple_string(self) -> str:
        data = await self.read_line()
        # assert data[0] == "+", "Expected + as bulk string payload identifier."
        return data

    async def read_simple_error(self) -> str:
        data = await self.read_line()
        # assert data[0] == "-", "Expected - as bulk string payload identifier."
        return data

    async def read_integer(self) -> int:
        data = await self.read_line()
        # assert data[0] == "-", "Expected - as bulk string payload identifier."
        return int(data)

    async def read_bulk_string(self) -> Optional[str]:
        metadata = await self.read_line()
        print(metadata)
        # assert metadata[0] == "$", "Expected $ as bulk string payload identifier."
        length = int(metadata)
        if length == -1:
            return None
        string = await self.read_until(length + 2)
        # We can't use readuntil(b"\r\n) over here, cause the content might
        # contain our seperator, and we would lose the data. Instead we need to
        # use the length to get the full data.
        return string

    async def read_line(self) -> str:
        data = await self.reader.readuntil(b"\r\n")
        print(data)
        return data[:-2].decode()

    async def read_until(self, n: int) -> str:
        data = await self.reader.readexactly(n)
        print(data)
        return data[:-2].decode()
