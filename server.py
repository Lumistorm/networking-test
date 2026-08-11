import socket
import threading
import struct


HOST = '192.168.18.44'
PORT = 8000
HEADER_SIZE = 10
HEADER_FORMAT = '!QH'
ENCODING_FORMAT = 'utf-8'


def create_server():
    port = PORT
    address = (HOST, port)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(address)

    return server


def start_server(server: socket.socket):
    server.listen()
    print('Server is listening...')

    while True:
        client_socket, client_address = server.accept()

        client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address))
        client_thread.start()


def handle_client(client_socket: socket.socket, client_address):
    print(f'{client_address} has connected')
    connected = True

    while connected:
        file_name = receive_file(client_socket)

        if file_name:
            print(f'Received file "{file_name}" from {client_address}')

    print(f'{client_address} has disconnected')
    client_socket.close()


def receive_message(client_socket):
    header = client_socket.recv(HEADER_SIZE)
    if len(header) < HEADER_SIZE:
        return None

    message_length, message_type = struct.unpack('!IB', header)

    message = b''
    while len(message) < message_length:
        packet = client_socket.recv(message_length)
        if not packet:
            return None
        message += packet

    return message


def receive_file(client_socket):
    header = client_socket.recv(HEADER_SIZE)
    if len(header) < HEADER_SIZE:
        return None

    file_size, file_name_length = struct.unpack(HEADER_FORMAT, header)
    packet = recv_all(client_socket, file_name_length)
    file_name = packet.decode(ENCODING_FORMAT)

    print('Receiving file')

    with open(f'server/{file_name}', 'wb') as file:
        bytes_received = 0
        while bytes_received < file_size:
            chunk_size = min(4096, file_size - bytes_received)
            chunk = client_socket.recv(chunk_size)

            file.write(chunk)
            bytes_received += len(chunk)

    return file_name


def recv_all(socket_conn, size):
    data = b''
    while len(data) < size:
        packet = socket_conn.recv(size - len(data))
        if not packet:
            return None

        data += packet

    return data


def main():
    server = create_server()
    start_server(server)


if __name__ == '__main__':
    main()
