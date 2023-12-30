from asyncio import StreamReader, StreamWriter
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

    async def read_array(self) -> Optional[list[Any]]:
        arr: list[Any] = []

        metadata = await self.read_line()
        if not metadata:
            return arr
        length = int(metadata)
        if length == -1:
            return None
        for _ in range(length):
            msg = await self.read_message()
            arr.append(msg)
        return arr

    async def read_simple_string(self) -> str:
        data = await self.read_line()
        return data

    async def read_simple_error(self) -> str:
        data = await self.read_line()
        return data

    async def read_integer(self) -> int:
        data = await self.read_line()
        return int(data)

    async def read_bulk_string(self) -> Optional[str]:
        metadata = await self.read_line()
        # print(metadata)
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
        # print(data)
        return data[:-2].decode()

    async def read_until(self, n: int) -> str:
        data = await self.reader.readexactly(n)
        # print(data)
        return data[:-2].decode()


class RESPWriter(object):
    def __init__(self, writer: StreamWriter):
        self.writer = writer

    async def serialize_array(self, arr: list[Any]) -> str:
        MSG_CODE, DELIMITER = "*", "\r\n"
        response = ""
        response += MSG_CODE + str(len(arr)) + DELIMITER
        for obj in arr:
            if type(obj) is str:
                response += await self.serialize_bulk_string(obj)
            elif type(obj) is int:
                response += await self.serialize_integer(obj)
            elif type(obj) is list:
                response += await self.serialize_array(obj)
        return response

    async def serialize_simple_string(self, data: str) -> str:
        MSG_CODE, DELIMITER = "+", "\r\n"
        message = MSG_CODE + data + DELIMITER
        return message

    async def serialize_simple_error(self, data: str) -> str:
        MSG_CODE, DELIMITER = "-", "\r\n"
        message = MSG_CODE + data + DELIMITER
        return message

    async def serialize_integer(self, data: int) -> str:
        MSG_CODE, DELIMITER = ":", "\r\n"
        message = MSG_CODE + str(data) + DELIMITER
        return message

    async def serialize_bulk_string(self, data: Optional[str]) -> str:
        MSG_CODE, DELIMITER = "$", "\r\n"
        if not data:  # Null bulk strings
            message = "$-1\r\n"
        else:
            message = MSG_CODE + str(len(data)) + DELIMITER + data + DELIMITER
        return message

    async def write_array(self, arr: list[Any]):
        message = await self.serialize_array(arr)
        await self.write(message)

    async def write_simple_string(self, data: str):
        message = await self.serialize_simple_string(data)
        await self.write(message)

    async def write_simple_error(self, data: str):
        message = await self.serialize_simple_error(data)
        await self.write(message)

    async def write_integer(self, data: int):
        message = await self.serialize_integer(data)
        await self.write(message)

    async def write_bulk_string(self, data: Optional[str]):
        message = await self.serialize_bulk_string(data)
        await self.write(message)

    async def write(self, message: str):
        self.writer.write(message.encode())
        await self.writer.drain()

    async def close(self):
        self.writer.write_eof()
        self.writer.close()
