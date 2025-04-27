import enum
import pickle
import struct
from typing import Tuple


class Message:
    header = ">B I"

    class Type(enum.Enum):
        SHUTDOWN = 1
        PIPELINE = 2

    @classmethod
    def header_size(cls) -> int:
        return struct.calcsize(cls.header)

    @classmethod
    def unpack(cls, data: bytes) -> object:
        return pickle.loads(data)
    
    @classmethod
    def pack(cls, mtype: Type, mbody: object) -> Tuple[bytes, int]:
        mbody = pickle.dumps(mbody)
        data = struct.pack(cls.header, mtype.value, len(mbody)) + mbody
        return data, len(mbody)