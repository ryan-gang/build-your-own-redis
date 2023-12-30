from app.expiry import EXPIRY_TIMESTAMP_DEFAULT_VAL


class RDBParser(object):
    def __init__(self, path: str):
        self.fhand = open(path, "rb")
        magic_string = self._read_bytes(5)
        assert magic_string.decode() == "REDIS"
        _ = self._read_bytes(4)  # version
        _ = self.parse_simple_dict()  # auxiliary_fields
        _ = self._read_bytes(2)  # db selector
        _ = self._read_bytes(1)  # resizedb field
        _ = self.parse_length_encoded_int()  # Hash table size
        _ = self.parse_length_encoded_int()  # Expiry table size
        self.kv = self.parse_dict_w_expiry()

    def _read_bytes(self, size: int) -> bytes:
        return self.fhand.read(size)

    def _peek_bytes(self) -> bytes:
        return self.fhand.peek(1)

    def parse_length_encoded_int(self) -> tuple[bool, int]:
        determinant = self._read_bytes(1)[0]
        determinant_bits = bin(determinant)[2:].zfill(8)

        special_format = False
        match determinant_bits[:2]:
            case "00":
                length = determinant & 0b00111111
            case "01":
                next_byte = self._read_bytes(1)[0]
                first_byte = determinant & 0b00111111
                length = (first_byte << 8) | next_byte
            case "10":
                length = int.from_bytes(self._read_bytes(4), byteorder="big")
            case "11":
                # The next object is encoded in a special format. The remaining 6 bits indicate the format.
                format = determinant & 0b00111111
                special_format = True
                match int(format):
                    case 0:
                        length = 1
                    case 1:
                        length = 2
                    case 2:
                        length = 4
                    case 3:
                        # Compressed string follows, Not implemented.
                        length = -1
                    case _:
                        raise ValueError(
                            f"Unknown format for Length Encoded Int case '11' : {int(format)}"
                        )
            case _:
                raise ValueError(
                    f"Unknown encoding for Length Encoded Int : {determinant_bits[2:]}"
                )

        return special_format, length

    def parse_encoded_string(self) -> str:
        special_format, length = self.parse_length_encoded_int()
        if not special_format:
            return self._read_bytes(length).decode()
        else:
            # This is the "Integers as String" path
            return str(int.from_bytes(self._read_bytes(length), byteorder="big"))

    def parse_simple_dict(self) -> dict[str, str]:
        d: dict[str, str] = {}
        while self._peek_bytes[:1] == b"\xfa":
            self._read_bytes(1)  # Skip
            key = self.parse_encoded_string()
            value = self.parse_encoded_string()
            d[key] = value
        return d

    def parse_dict_w_expiry(self):
        d: dict[str, tuple[str, int]] = {}
        # key -> (value, expiry_timestamp)
        while self._peek_bytes[:1] != b"\xff":
            if self._peek_bytes[:1] == b"\xfc":
                # "expiry time in ms", followed by 8 byte unsigned long
                self._read_bytes(1)  # Skip
                expiry = int.from_bytes(self._read_bytes(8), byteorder="little")
            elif self._peek_bytes[:1] == b"\xfd":
                # "expiry time in seconds", followed by 4 byte unsigned int
                self._read_bytes(1)  # Skip
                expiry = int.from_bytes(self._read_bytes(8), byteorder="little") * 1000
            else:
                expiry = EXPIRY_TIMESTAMP_DEFAULT_VAL
            _ = self._read_bytes(1)  # value_type
            key = self.parse_encoded_string()
            value = self.parse_encoded_string()
            d[key] = (value, expiry)

        return d
