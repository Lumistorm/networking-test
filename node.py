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
        self.address = (host, port)

        self.peers = {}
        self.discovered_peers = set()

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        self.running = False

    def start(self):
        self.running = True

        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()

        self.discovery_socket.bind(('', 5000))

        self.broadcast_presence()
        threading.Thread(target=self.accept_loop, daemon=True).start()
        threading.Thread(target=self.discovery_loop, daemon=True).start()

    def accept_loop(self):
        while self.running:
            connection, address = self.server_socket.accept()
            threading.Thread(
                target=self.handle_connection,
                args=(connection, address),
                daemon=True
            ).start()

    def handle_connection(self, connection, address):
        self.register_peer(connection, address)

        print(f'Connected to {':'.join(map(str, address))}')
        try:
            while self.running:
                header_dict, data = self.receive_packet(connection)
                if not data:
                    break

                print(f'[{':'.join(map(str, address))}] {data.decode('utf-8')}')
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

            threading.Thread(
                target=self.handle_connection,
                args=(sock, address),
                daemon=True
            ).start()
        except OSError as e:
            print(f'Connection to {':'.join(map(str, address))} failed: {e}')

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

    def broadcast_presence(self):
        data_dict = {'address': self.address}
        message = json.dumps(data_dict).encode('utf-8')
        self.discovery_socket.sendto(message, ('<broadcast>', 5000))

    def discovery_loop(self):
        try:
            while self.running:
                data, _ = self.discovery_socket.recvfrom(4096)
                data_dict = json.loads(data.decode('utf-8'))

                address = tuple(data_dict['address'])

                if address == self.address:
                    continue
                if address in self.discovered_peers:
                    continue

                self.discovered_peers.add(address)
                print(f'Discovered {':'.join(map(str, address))}')
        except OSError as e:
            print(f'Socket closed unexpectedly: {e}')


def create_node(port):
    host = get_local_ip()

    node = Node(host=host, port=port)

    return node


def main():
    port = random.randint(2000, 8000)
    my_node = create_node(port)
    my_node.start()


if __name__ == '__main__':
    main()
