import struct
import json
from enum import StrEnum


class MessageType(StrEnum):
    PING = 'ping'
    PONG = 'pong'

    AUTH = 'auth'

    DATA = 'data'

    ERROR = 'error'


def _handle_packet():
    pass


def _frame_header(header):
    header_json = json.dumps(header)
    header_bytes = header_json.encode('utf-8')
    header_size_bytes = struct.pack('!I', len(header_bytes))

    return header_size_bytes + header_bytes


def _send_packet(connection, header, payload=None):
    payload = payload if payload is not None else b''

    header['size'] = len(payload)
    framed_header = _frame_header(header)

    connection.sendall(framed_header + payload)


def _send_stream(connection, header, chunks, size):
    header['size'] = size
    framed_header = _frame_header(header)

    connection.sendall(framed_header)

    for chunk in chunks:
        connection.sendall(chunk)


def receive_packet(connection):
    header_size_bytes = _receive_exactly(connection, 4)
    header_size, = struct.unpack('!I', header_size_bytes)

    header_bytes = _receive_exactly(connection, header_size)
    header_json = header_bytes.decode('utf-8')
    header = json.loads(header_json)

    payload = _receive_exactly(connection, header['size'])

    return header, payload


def _receive_exactly(connection, size):
    data = bytearray()

    while len(data) < size:
        remaining = size - len(data)
        chunk = connection.recv(remaining)

        if not chunk:
            return None

        data.extend(chunk)

    return bytes(data)


def ping(connection):
    header = {'type': MessageType.PING}
    _send_packet(connection, header, None)


def _pong(connection):
    header = {'type': MessageType.PONG}
    _send_packet(connection, header, None)
