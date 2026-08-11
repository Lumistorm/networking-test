import socket
import threading
import time
import json
import struct
import os


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

    def start(self, protocol):
        self.server_socket = socket.socket(socket.AF_INET, protocol)
        self.server_socket.bind((self.host, self.port))

        while self.running:
            connection, address = self.server_socket.accept()

            peer_thread = threading.Thread(target=self.handle_peer, args=(connection, address), daemon=True)
            peer_thread.start()

    def handle_peer(self, connection, address):
        while self.running:
            data = connection.recv(4096)
            if not data:
                break

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

            peer_thread = threading.Thread(target=self.handle_peer, args=(sock, address), daemon=True)
            peer_thread.start()
        except OSError as e:
            print(f'Connection to {address} failed: {e}')

    def register_peer(self, connection, address):
        self.peers[address] = connection

    def remove_peer(self, address):
        self.peers.pop(address)

    def send_packet(self, connection, payload, header_dict=None):
        header_dict = {
            **(header_dict or {}),
            'size': len(payload),
            'timestamp': time.perf_counter()
        }
        header_bytes = json.dumps(header_dict).encode('utf-8')
        header_size = struct.pack('!I', len(header_bytes))

        connection.sendall(header_size + header_bytes + payload)

    def send_message(self, connection, message):
        message_bytes = message.encode('utf-8')
        header_dict = {
            'type': 'message',
        }

        self.send_packet(connection, payload=message_bytes, header_dict=header_dict)

    def send_file(self, connection, file_path):
        file_size = os.path.getsize(file_path)
        filename = os.path.basename(file_path)
        filename_bytes = filename.encode('utf-8')

        with open(file_path, 'rb') as file:
            while True:
                chunk = file.read(4096)
                if not chunk:
                    break

                header_dict = {
                    'type': 'message',
                }

                self.send_packet(connection, payload=message_bytes, header_dict=header_dict)

    def receive_exactly(self, connection, size: int):
        data = bytearray()

        while len(data) < size:
            remaining = size - len(data)
            chunk = connection.recv(remaining)
            if not chunk:
                return None

            data.extend(chunk)

        return bytes(data)





def create_node():
    host = get_local_ip()
    port = 8000

    node = Node(host=host, port=port)

    return node


def main():
    my_node = create_node()


if __name__ == '__main__':
    main()
