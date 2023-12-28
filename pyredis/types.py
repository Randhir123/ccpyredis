from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


@dataclass
class SimpleString:
    data: str

    def resp_encode(self):
        return f'+{self.data}\r\n'.encode()

    def as_str(self):
        return self.data


@dataclass
class Error:
    data: str

    def resp_encode(self):
        return f'-{self.data}\r\n'.encode()

    def as_str(self):
        return self.data


@dataclass
class Integer:
    value: int

    def resp_encode(self):
        return f':{self.value}\r\n'.encode()

    def as_str(self):
        return str(self.value)


@dataclass
class BulkString:
    data: bytes

    def resp_encode(self):
        if self.data == '':
            return f'${0}\r\n\r\n'.encode()
        elif self.data is None:
            return f'${-1}\r\n'.encode()
        return f'${len(self.data)}\r\n{self.data}\r\n'.encode()

    def as_str(self):
        return str(self.data.decode())

    def file_encode(self):
        if self.data is None:
            return b"$-1\r\n"

        return f"${len(self.data)}\r\n{self.data.decode()}\r\n".encode()


@dataclass
class Array(Sequence):
    data: list[Any]

    def resp_encode(self):
        if self.data is None:
            return f'*-1\r\n'.encode()
        elif len(self.data) == 0:
            return f'*0\r\n'.encode()
        else:
            s = f'*{len(self.data)}\r\n'.encode()
            xs = bytearray(s)
            for t in self.data:
                xs.extend(t.resp_encode())
            return bytes(xs)

    def __getitem__(self, index: int) -> Any:
        return self.data[index]

    def __len__(self) -> int:
        return len(self.data)

    def as_str(self):
        return '[' + ','.join([str(s) for s in self.data]) + ']'
