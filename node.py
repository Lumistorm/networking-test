import socket
import threading
import time
import json
import struct
import os
import random


def get_local_ip():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        sock.connect(('8.8.8.8', 80))
        local_ip = sock.getsockname()[0]
    except OSError:
        local_ip = '127.0.0.1'
    finally:
        sock.close()

    return local_ip


class Node:
    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.peers = {}

        self.server_socket = None
        self.running = True

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))

        self.server_socket.listen()

        threading.Thread(target=self.accept_loop, daemon=True).start()

    def accept_loop(self):
        while self.running:
            connection, address = self.server_socket.accept()
            threading.Thread(target=self.handle_connection, args=(connection, address), daemon=True).start()

    def handle_connection(self, connection, address):
        try:
            while self.running:
                data = self.receive_packet(connection)
                if not data:
                    break
        except ConnectionError as e:
            print(f'Peer disconnected: {e}')
        finally:
            connection.close()
            self.remove_peer(address)

    def connect(self, host: str, port: int):
        address = (host, port)

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            sock.settimeout(10)
            sock.connect(address)
            sock.settimeout(None)

            self.register_peer(sock, address)

            peer_thread = threading.Thread(target=self.handle_connection, args=(sock, address), daemon=True)
            peer_thread.start()
        except OSError as e:
            print(f'Connection to {address} failed: {e}')

    def register_peer(self, connection, address):
        self.peers[address] = connection

    def remove_peer(self, address):
        self.peers.pop(address, None)

    def send_packet(self, connection, data, header_dict):
        header_bytes = json.dumps(header_dict).encode('utf-8')
        header_size = struct.pack('!I', len(header_bytes))

        connection.sendall(header_size + header_bytes + data)

    def send_stream(self, connection, chunks, header_dict):
        header_bytes = json.dumps(header_dict).encode('utf-8')
        header_size = struct.pack('!I', len(header_bytes))

        connection.sendall(header_size + header_bytes)

        for chunk in chunks:
            connection.sendall(chunk)

    def send_message(self, connection, message):
        message_bytes = message.encode('utf-8')
        header_dict = {
            'type': 'message',
            'size': len(message_bytes),
            'timestamp': time.perf_counter()
        }

        self.send_packet(connection, data=message_bytes, header_dict=header_dict)

    def send_file(self, connection, file_path):
        file_size = os.path.getsize(file_path)
        filename = os.path.basename(file_path)

        header_dict = {
            'type': 'file',
            'filename': filename,
            'size': file_size,
            'timestamp': time.perf_counter()
        }

        with open(file_path, 'rb') as file:
            chunks = iter(lambda: file.read(4096), b'')

            self.send_stream(connection, chunks=chunks, header_dict=header_dict)

    def receive_packet(self, connection):
        header_size_bytes = self.receive_exactly(connection, size=4)
        header_size = struct.unpack('!I', header_size_bytes)[0]

        header_json = self.receive_exactly(connection, size=header_size).decode('utf-8')
        header_dict = json.loads(header_json)

        payload = self.receive_exactly(
            connection,
            size=header_dict['size']
        )

        return header_dict, payload

    def receive_exactly(self, connection, size: int):
        data = bytearray()

        while len(data) < size:
            remaining = size - len(data)
            chunk = connection.recv(remaining)
            if not chunk:
                raise ConnectionError

            data.extend(chunk)

        return bytes(data)

def create_node(port):
    host = get_local_ip()

    node = Node(host=host, port=port)

    return node


def main():
    port = random.randint(2000, 8000)
    my_node = create_node(port)
    my_node.start()
    host = input('Connect to: ')
    # my_node.connect(host, 8000)


if __name__ == '__main__':
    main()
