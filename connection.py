import socket
import struct
import json
from protocol import build_header, MessageType


def create_listening_socket(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    sock.bind((host, port))
    sock.listen()

    return sock


def connect(host, port, timeout):
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.settimeout(timeout)

    try:
        connection.connect((host, port))
    except OSError:
        connection.close()
        raise

    connection.settimeout(None)

    return connection


def close_connection(connection):
    try:
        connection.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass

    connection.close()
    

def ping(connection):
    send(connection, MessageType.PING, None)


def pong(connection):
    send(connection, MessageType.PONG, None)
    
    
def send(connection, message_type, session_id, payload=None):
    payload = payload if payload is not None else b''
    header = build_header(message_type, session_id, len(payload))

    connection.sendall(header + payload)


# def send_stream(connection, header, chunks, size):
#     header['size'] = size
#     framed_header = build_header(header)
#
#     connection.sendall(framed_header)
#
#     for chunk in chunks:
#         connection.sendall(chunk)


def receive(connection):
    header_size_bytes = _receive_exactly(connection, 4)
    header_size, = struct.unpack('!I', header_size_bytes)

    header_bytes = _receive_exactly(connection, header_size)
    header_json = header_bytes.decode('utf-8')
    header = json.loads(header_json)

    payload = _receive_exactly(connection, header['payload_length'])

    return header, payload


def _receive_exactly(connection, size):
    data = bytearray()

    while len(data) < size:
        remaining = size - len(data)
        chunk = connection.recv(remaining)

        if chunk is None:
            return None

        data.extend(chunk)

    return bytes(data)

