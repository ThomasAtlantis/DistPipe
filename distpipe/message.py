import pickle
import struct
from typing import Tuple


class Message:
    header = ">32s I"

    @staticmethod
    def unpack(data: bytes) -> object:
        return pickle.loads(data)
    
    @staticmethod
    def pack(mtype: str, mbody: object) -> Tuple[bytes, int]:
        mbody = pickle.dumps(mbody)
        data = struct.pack(Message.header, mtype.encode('utf-8'), len(mbody)) + mbody
        return data, len(mbody)