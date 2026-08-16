import struct
import json
from enum import StrEnum


class MessageType(StrEnum):
    HELLO = 'HELLO'
    HELLO_ACK = 'HELLO_ACK'

    PING = 'PING'
    PONG = 'PONG'

    TEXT = 'TEXT'
    DATA = 'DATA'
    FILE = 'FILE'
    STREAM_CHUNK = 'STREAM_CHUNK'

    ERROR = 'ERROR'

    AUTH = 'AUTH'


def handle_packet():
    pass


def build_header(message_type, connection_id, payload_length):
    header = {
        'message_type': message_type.value,
        'session_id': connection_id,
        'payload_length': payload_length
    }

    header_json = json.dumps(header)
    header_bytes = header_json.encode('utf-8')
    header_size_bytes = struct.pack('!I', len(header_bytes))

    return header_size_bytes + header_bytes
