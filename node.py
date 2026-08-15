import threading
import time
import os
import random
from connection import *
from protocol import *


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

        self.server_socket = create_tcp_socket(host, port)
        self.discovery_socket = create_udp_socket('', 5000)

        self.running = False

    def start(self):
        self.running = True

        threading.Thread(target=self.accept_loop, daemon=True).start()
        threading.Thread(target=self.discovery_loop, daemon=True).start()

        self.broadcast_presence()

    def stop(self):
        self.running = False
        self.server_socket.close()
        self.discovery_socket.close()

        for connection in list(self.peers.values()):
            connection.close()

        self.peers.clear()

    def connect(self, host: str, port: int):
        connect(host, port, 10)

    def disconnect(self, address):
        connection = self.peers.get(address)

        if connection is None:
            return False

        close_connection(connection)
        self.remove_peer(address)

        return True

    def accept_loop(self):
        try:
            while self.running:
                connection, address = self.server_socket.accept()
                threading.Thread(
                    target=self.handle_connection,
                    args=(connection, address),
                    daemon=True
                ).start()

        except OSError as e:
            if self.running:
                print_info(f'Socket closed unexpectedly: {e}')

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
                print_info(f'Discovered {':'.join(map(str, address))}')
        except OSError as e:
            if self.running:
                print_info(f'Socket closed unexpectedly: {e}')

    def broadcast_presence(self):
        data_dict = {'address': self.address}
        message = json.dumps(data_dict).encode('utf-8')
        self.discovery_socket.sendto(message, ('<broadcast>', 5000))

    def handle_connection(self, connection, address):
        self.register_peer(connection, address)

        address_string = ':'.join(map(str, address))
        print_info(f'Connected to {address_string}')
        try:
            while self.running:
                header_dict, data = receive_packet(connection)
                if not data:
                    break

                print_info(f'[{address_string}] {data.decode('utf-8')}')
        except (ConnectionError, OSError):
            print_info(f'{address_string} disconnected')
        finally:

            connection.close()
            self.remove_peer(address)

    def register_peer(self, connection, address):
        self.peers[address] = connection

    def remove_peer(self, address):
        self.peers.pop(address, None)

    def send_message(self, connection, message):
        message_bytes = message.encode('utf-8')
        header = {
            'type': 'message',
            'timestamp': time.time()
        }

        send_packet(connection, header=header, payload=message_bytes)

    def send_file(self, connection, file_path):
        file_size = os.path.getsize(file_path)
        filename = os.path.basename(file_path)

        header = {
            'type': 'file',
            'filename': filename,
            'timestamp': time.time()
        }

        with open(file_path, 'rb') as file:
            chunks = iter(lambda: file.read(4096), b'')
            send_stream(connection, header=header, chunks=chunks, size=file_size)


def create_node(port):
    host = get_local_ip()
    node = Node(host=host, port=port)

    return node


def print_error(error):
    print(f'\033[31m{error}\033[0m', flush=True)
    print('> ', end='', flush=True)


def print_info(message):
    print(f'\r{message}', flush=True)
    print('\r> ', end='', flush=True)


def handle_commands(node):
    print('\r> ', end='', flush=True)
    command = input('').strip()
    if command.startswith('connect '):
        try:
            host, port = command[8:].split(':')
        except ValueError:
            print_error('Invalid arguments. Expected \'connect <host>:<port>\'')
            return

        try:
            port = int(port)
        except ValueError:
            print_error('Port must be an integer')
            return

        node.connect(host, port)
    elif command.startswith('send '):
        try:
            args = command[5:].split(maxsplit=1)
            address, message = args
            host, port = address.split(':')
        except ValueError:
            print_error('Invalid arguments. Expected \'send <host>:<port> <message>\'')
            return

        try:
            port = int(port)
        except ValueError:
            print_error('Port must be an integer')
            return

        connection = node.peers.get((host, port))
        if connection is None:
            print_error(f'Message failed: not connected to {host}:{port}')
            return

        node.send_message(connection, message)
    elif command.startswith('disconnect '):
        try:
            address = command[11:]
            host, port = address.split(':')
        except ValueError:
            print_error('Invalid arguments. Expected \'disconnect <host>:<port>\'')
            return

        try:
            port = int(port)
        except ValueError:
            print_error('Port must be an integer')
            return

        print_info((host, port))
        disconnected = node.disconnect((host, port))

        if disconnected:
            print_info(f'Disconnected from {address}')
        else:
            print_error(f'Disconnect failed: not connected to {address}')

    elif command.startswith('stop'):
        node.stop()
    else:
        print_error(f'Unknown command: {command}')
        return


def main():
    port = random.randint(2000, 8000)
    print_info(port)
    my_node = create_node(port)
    my_node.start()
    while my_node.running:
        handle_commands(my_node)


if __name__ == '__main__':
    main()
