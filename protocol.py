import struct
import json
from enum import Enum


class MessageType(Enum):
    HELLO = 'HELLO'
    HELLO_ACK = 'HELLO_ACK'

    PING = 'PING'
    PONG = 'PONG'

    TEXT = 'TEXT'
    DATA = 'DATA'
    FILE = 'FILE'

    ERROR = 'ERROR'

    AUTH = 'AUTH'


def handle_packet():
    pass


def _build_header(header):
    header_json = json.dumps(header)
    header_bytes = header_json.encode('utf-8')
    header_size_bytes = struct.pack('!I', len(header_bytes))

    return header_size_bytes + header_bytes


