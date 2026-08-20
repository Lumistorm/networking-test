import json
import socket
import struct
import time

from protocol import MessageType, build_header, parse_header


class Connection:
    def __init__(self, sock, *, is_inbound):
        self.sock = sock
        self.connection_id = ''

        self.is_inbound = is_inbound
        self._next_stream_id = 2 if is_inbound else 1

        self.rtt_ms = 0

    def close(self):
        self.sock.close()

    def disconnect(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        self.sock.close()

    def handshake(self, node_id):
        if not self.is_inbound:
            self.send_hello(node_id)

            header, payload = self.receive()
            if header['type'] != MessageType.HELLO_ACK:
                raise ConnectionError

            metadata = json.loads(payload.decode('utf-8'))
            peer_node_id = metadata['node_id']

            self.send_hello_done()
        else:
            header, payload = self.receive()
            if header['type'] != MessageType.HELLO:
                raise ConnectionError

            metadata = json.loads(payload.decode('utf-8'))
            peer_node_id = metadata['node_id']

            self.send_hello_ack(node_id)

            header, payload = self.receive()
            if header['type'] != MessageType.HELLO_DONE:
                raise ConnectionError

        return peer_node_id

    def send_hello(self, node_id):
        metadata = {
            'node_id': node_id,
        }
        payload = json.dumps(metadata).encode('utf-8')

        self.send(MessageType.HELLO, payload)

    def send_hello_ack(self, node_id):
        metadata = {
            'node_id': node_id,
        }
        payload = json.dumps(metadata).encode('utf-8')

        self.send(MessageType.HELLO_ACK, payload)

    def send_hello_done(self):
        self.send(MessageType.HELLO_DONE)

    def ping(self):
        metadata = {
            'ping_sent_time': time.perf_counter()
        }
        payload = json.dumps(metadata).encode('utf-8')

        self.send(MessageType.PING, payload)

    def pong(self, ping_metadata):
        metadata = {
            **ping_metadata,
            'pong_sent_time': time.perf_counter()
        }
        payload = json.dumps(metadata).encode('utf-8')

        self.send(MessageType.PONG,payload)

    def send(self, message_type, payload=b'', *, stream_id=None):
        header = build_header(
            message_type=message_type,
            payload_length=len(payload),
            connection_id=self.connection_id,
            stream_id=stream_id
        )

        self.sock.sendall(header + payload)

    def send_stream(self, chunks, metadata):
        stream_id = self.new_stream_id()

        metadata_bytes = json.dumps(metadata).encode()
        self.send(MessageType.STREAM_START, metadata_bytes, stream_id=stream_id)

        for chunk in chunks:
            self.send(MessageType.DATA, chunk, stream_id=stream_id)

        self.send(MessageType.STREAM_END, stream_id=stream_id)

    def new_stream_id(self):
        stream_id = self._next_stream_id
        self._next_stream_id += 2

        return stream_id

    def receive(self):
        header_length_bytes = self._receive_exactly(4)
        header_length = struct.unpack('!I', header_length_bytes)[0]

        header_bytes = self._receive_exactly(header_length)
        header = parse_header(header_bytes)

        payload = self._receive_exactly(header['length'])

        return header, payload

    def _receive_exactly(self, size):
        data = bytearray()

        while len(data) < size:
            remaining = size - len(data)
            chunk = self.sock.recv(remaining)

            if not chunk:
                raise ConnectionError('Peer disconnected')

            data.extend(chunk)

        return bytes(data)


def connect(host, port, timeout):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)

    try:
        sock.connect((host, port))
    except OSError:
        sock.close()
        raise

    sock.settimeout(None)

    return Connection(sock, is_inbound=False)


def create_listening_socket(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    sock.bind((host, port))
    sock.listen()

    return sock
