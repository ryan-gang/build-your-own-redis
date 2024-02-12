from asyncio import StreamReader, StreamWriter
from typing import Any, Optional


class RESPReader(object):
    """
    A class for reading and parsing Redis RESP commands from clients efficiently
    and in real-time.

    The reader operates by reading data chunks from a stream and parsing them
    simultaneously based on the first byte encountered (payload identifier).
    This approach minimizes memory usage while maintaining fast processing and
    suitability for real-time applications.
    """

    def __init__(self, reader: StreamReader):
        """
        Initializes the `RESPReader` instance with the provided `reader` object.
        """
        self.reader = reader

    def get_byte_offset(self, message: list[str]) -> int:
        # Returns the byte offset for a RESP command
        # To be used only with RESP Arrays
        offset = 0
        offset += 2 * (2 * len(message) + 1)
        offset += len(str(len(message))) + 1
        for _, val in enumerate(message):
            msg_len = len(val)
            offset += len(str(msg_len)) + 1
            offset += msg_len
        return offset

    async def read_message(self) -> Any:
        """
        Reads and parses a single RESP message from the underlying stream.

        This method first reads one byte to identify the message type based on the RESP protocol encoding. It then delegates the actual parsing to the appropriate internal function based on the identified type.
        """
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

    async def read_array(self, skip_first_byte: bool = False) -> Optional[list[Any]]:
        """
        Reads and parses a RESP array message from the stream.

        This method assumes the identifier byte has already been identified as
        the indicator for an array message. It then reads the number of elements
        in the array and parses each element individually using `read_message()`.
        """
        arr: list[Any] = []
        if skip_first_byte:
            await self.read_until(1)

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
        """
        Reads and parses a RESP simple string message from the stream.

        This method assumes the identifier byte has already been identified as
        the indicator for a simple string. It then reads the actual string data.
        """
        data = await self.read_line()
        return data

    async def read_simple_error(self) -> str:
        """
        Reads and parses a RESP simple error message from the stream.

        This method assumes the identifier byte has already been identified as
        the indicator for a simple error. It then reads the remaining content of
        the error message.
        """
        data = await self.read_line()
        return data

    async def read_integer(self) -> int:
        """
        Reads and parses a RESP integer message from the stream.

        This method assumes the identifier byte has already been identified as
        the indicator for an integer. It then reads the remaining bytes
        representing the integer value and converts it to a Python integer.
        """
        data = await self.read_line()
        return int(data)

    async def read_bulk_string(self) -> Optional[str]:
        """
        Reads and parses a RESP bulk string message from the stream.

        This method assumes the identifier byte has already been identified as
        the indicator for a bulk string. It then reads the length of the string
        data and reads the actual bytes.
        """
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
        """
        Reads a single RESP line message from the stream.

        This method reads bytes from the underlying stream until it encounters a
        CRLF character (`b"\r\n"`). It then trims the trailing CRLF
        characters (`\r\n`) and decodes the remaining bytes using the default
        encoding.
        """
        data = await self.reader.readuntil(b"\r\n")
        return data[:-2].decode()

    async def read_until(self, n: int) -> str:
        """
        Reads a specific number of bytes from the stream.

        This method directly calls the `readexactly` method of the underlying
        stream reader and expects an exact number of bytes (`n`) to be provided.
        It then trims the trailing newline characters (`\r\n`) and decodes the
        remaining bytes using the default encoding.
        """
        data = await self.reader.readexactly(n)
        return data[:-2].decode()

    async def read_rdb(self) -> bytes:
        """
        Reads a specific number of bytes from the stream.

        This method directly calls the `readexactly` method of the underlying
        stream reader and expects an exact number of bytes (`n`) to be provided.
        It then trims the trailing newline characters (`\r\n`) and decodes the
        remaining bytes using the default encoding.
        """
        _ = await self.reader.readexactly(1)
        l = await self.read_line()
        length = int(l)
        if length == -1:
            return b""
        data = await self.reader.readexactly(length)
        return data


class RESPWriter(object):
    """
    A class for serializing and writing RESP commands for sending responses to
    Redis clients.

    The writer utilizes an underlying `StreamWriter` object to write byte
    encoded messages based on the given data and desired RESP type.
    """

    def __init__(self, writer: StreamWriter):
        """
        Initializes the `RESPWriter` instance with the provided `writer` object.
        """
        self.writer = writer

    async def serialize_array(self, arr: list[Any]) -> str:
        """
        Serializes a list of data elements into a RESP array encoded message.

        This method iterates through the provided list and serializes each
        element based on its type (string, integer, or nested array) using the
        corresponding helper functions (`serialize_simple_string`,
        `serialize_integer`, or recursively calling `serialize_array`). The
        final message is prefixed with the RESP array code ("*") and the length
        of the array.
        """
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
        """
        Serializes a string into a RESP simple string encoded message.
        """
        MSG_CODE, DELIMITER = "+", "\r\n"
        message = MSG_CODE + data + DELIMITER
        return message

    async def serialize_simple_error(self, data: str) -> str:
        """
        Serializes a string into a RESP simple error encoded message.
        """
        MSG_CODE, DELIMITER = "-", "\r\n"
        message = MSG_CODE + data + DELIMITER
        return message

    async def serialize_integer(self, data: int) -> str:
        """
        Serializes an integer into a RESP integer encoded message.
        """
        MSG_CODE, DELIMITER = ":", "\r\n"
        message = MSG_CODE + str(data) + DELIMITER
        return message

    async def serialize_bulk_string(self, data: Optional[str]) -> str:
        """
        Serializes a string into a RESP bulk string encoded message.

        This method checks for null bulk strings (represented by `None`) and
        encodes them with a special code ("$-1\r\n"). Otherwise, it prefixes the
        string length with the RESP bulk string code ("$") and appends the
        string data and a newline delimiter ("\r\n").
        """
        MSG_CODE, DELIMITER = "$", "\r\n"
        if not data:  # Null bulk strings
            message = "$-1\r\n"
        else:
            message = MSG_CODE + str(len(data)) + DELIMITER + data + DELIMITER
        return message

    async def write_array(self, arr: list[Any]):
        """
        Writes a RESP array encoded message to the underlying stream.
        """
        message = await self.serialize_array(arr)
        await self.write(message)

    async def write_simple_string(self, data: str):
        """
        Writes a RESP simple string encoded message to the underlying stream.
        """
        message = await self.serialize_simple_string(data)
        await self.write(message)

    async def write_simple_error(self, data: str):
        """
        Writes a RESP simple error encoded message to the underlying stream.
        """
        message = await self.serialize_simple_error(data)
        await self.write(message)

    async def write_integer(self, data: int):
        """
        Writes a RESP integer encoded message to the underlying stream.
        """
        message = await self.serialize_integer(data)
        await self.write(message)

    async def write_bulk_string(self, data: Optional[str]):
        """
        Writes a RESP bulk string encoded message to the underlying stream.
        """
        message = await self.serialize_bulk_string(data)
        await self.write(message)

    async def write(self, message: str):
        """
        Writes a serialized RESP message to the underlying stream.

        This method encodes the provided `message` string (assumed to be already
        in RESP format) into bytes and writes it to the `writer` object. It then
        waits for the stream to be flushed to ensure all data is sent.
        """
        self.writer.write(message.encode())
        await self.writer.drain()

    async def write_raw(self, message: bytes):
        """ """
        self.writer.write(message)
        await self.writer.drain()

    async def close(self):
        """
        Properly closes the underlying stream connection.

        This method first sends an end-of-file signal (`write_eof`) to the
        writer and then closes the connection. This ensures any remaining data
        is sent and the connection is gracefully closed.
        """
        self.writer.write_eof()
        self.writer.close()
