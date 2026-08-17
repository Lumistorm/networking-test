import threading
import secrets
import random
import json
from connection import *


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

        self.node_id = secrets.token_hex(8)
        print(self.node_id)

        self.connected_peers = {}
        self.known_peers = {}

        self.running = False

        self.tcp_socket = create_listening_socket(host, port)
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.udp_socket.bind(('', 5000))

        self._accept_thread = None
        self._discovery_thread = None

    def start(self):
        if self.running:
            return

        self.running = True

        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            daemon=True
        )
        self._discovery_thread = threading.Thread(
            target=self._discovery_loop,
            daemon=True
        )

        self._accept_thread.start()
        self._discovery_thread.start()

        self.broadcast_presence()

    def stop(self):
        if not self.running:
            return

        self.running = False

        self.tcp_socket.close()
        self.udp_socket.close()

        self._accept_thread.join()
        self._discovery_thread.join()

        self._accept_thread = None
        self._discovery_thread = None

    def connect(self, node_id):
        peer = self.known_peers[node_id]
        host = peer['host']
        port = peer['port']
        connection = connect(host, port, timeout=10)

        thread = threading.Thread(
            target=self._handle_connection,
            args=(connection,),
            daemon=True
        )
        thread.start()

    def disconnect(self, node_id):
        connection = self.connected_peers.pop(node_id, None)

        if connection is None:
            return False

        connection.disconnect()

        return True

    def _handle_connection(self, connection):
        try:
            peer_node_id = connection.handshake(self.node_id)
            self.connected_peers[peer_node_id] = connection

            print(f'Connected to {peer_node_id}')

            while self.running:
                message_type, data = self.receive(connection)
                if data is None:
                    break

                print_info(f'[{peer_node_id}] {data}')

                self._handle_packet()
        except OSError:
            if self.running:
                raise
        finally:
            connection.close()

    def _accept_loop(self):
        while self.running:
            try:
                sock, address = self.tcp_socket.accept()
            except OSError:
                if self.running:
                    raise
                break

            connection = Connection(sock, is_inbound=True)

            thread = threading.Thread(
                target=self._handle_connection,
                args=(connection,),
                daemon=True
            )
            thread.start()

    def _discovery_loop(self):
        while self.running:
            try:
                data, _ = self.udp_socket.recvfrom(4096)
                data_dict = json.loads(data.decode('utf-8'))

                node_id = data_dict['node_id']
                host = data_dict['host']
                port = data_dict['port']
                address = (host, port)

                if address == self.address:
                    continue
                if node_id in self.known_peers:
                    continue

                self.known_peers[node_id] = {
                    'host': host,
                    'port': port,
                }
                print_info(f'Discovered {node_id}')
                self.broadcast_presence()
            except OSError:
                if self.running:
                    raise
                break

    def broadcast(self, message):
        message = message.encode('utf-8')

        self.udp_socket.sendto(
            message,
            ('<broadcast>', 5000)
        )

    def broadcast_presence(self):
        data_dict = {
            'node_id': self.node_id,
            'host': self.host,
            'port': self.port,
        }
        message = json.dumps(data_dict)
        self.broadcast(message)

    def _add_peer(self, node_id):
        self.known_peers.setdefault(node_id)

    def _remove_peer(self, node_id):
        self.known_peers.pop(node_id, None)

    def _handle_packet(self):
        pass

    def send_message(self, node_id, message):
        message_bytes = message.encode('utf-8')
        connection = self.connected_peers.get(node_id)

        if connection is None:
            print_error(f'Message error: Not connected to {node_id}')

            return

        connection.send(
            message_type=MessageType.TEXT,
            payload=message_bytes
        )

    # def send_file(self, node_id, file_path):
    #     file_size = os.path.getsize(file_path)
    #     filename = os.path.basename(file_path)
    #
    #     header = {
    #         'type': 'file',
    #         'filename': filename,
    #         'size': file_size,
    #         'timestamp': time.time()
    #     }
    #
    #     connection = self.connected_peers[node_id]
    #
    #     with open(file_path, 'rb') as file:
    #         chunks = iter(lambda: file.read(4096), b'')
    #         send_stream(connection, chunks=chunks, header=header)

    def receive(self, connection):
        message_type, payload = connection.receive()
        return message_type, payload.decode('utf-8')


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
        node_id = command[8:]

        node.connect(node_id)
    elif command.startswith('send '):
        args = command[5:].split(maxsplit=1)
        try:
            node_id, message = args
        except ValueError:
            print_error('Invalid arguments. Expected \'send <node_id> <message>\'')
            return

        node.send_message(node_id, message)
    elif command.startswith('disconnect '):
        node_id = command[11:]

        disconnected = node.disconnect(node_id)

        if disconnected:
            print_info(f'Disconnected from {node_id}')
        else:
            print_error(f'Disconnect failed: not connected to {node_id}')

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
