import struct
import json
from enum import StrEnum


class MessageType(StrEnum):
    HELLO = 'HELLO'
    HELLO_ACK = 'HELLO_ACK'
    HELLO_DONE = 'HELLO_DONE'

    PING = 'PING'
    PONG = 'PONG'

    TEXT = 'TEXT'
    DATA = 'DATA'
    FILE = 'FILE'
    STREAM_START = 'STREAM_START'
    STREAM_END = 'STREAM_END'

    ERROR = 'ERROR'

    AUTH = 'AUTH'


def handle_packet():
    pass


def build_header(message_type, payload_length, connection_id, stream_id=None):
    header = {
        'type': message_type.value,
        'length': payload_length,
        'connection_id': connection_id,
        'stream_id': stream_id,
    }

    header_json = json.dumps(header)
    header_bytes = header_json.encode('utf-8')
    header_size = struct.pack('!I', len(header_bytes))

    return header_size + header_bytes


def parse_header(header_bytes):
    header_json = header_bytes.decode('utf-8')
    header = json.loads(header_json)

    header['type'] = MessageType(header['type'])

    return header
