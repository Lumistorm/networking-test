import socket
import struct
import os
import time


HOST = '192.168.18.41'
PORT = 8000
HEADER_FORMAT = '!QH'
ENCODING_FORMAT = 'utf-8'


def connect_to_host(host_name: str):
    address = (host_name, PORT)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(address)

    return client


def send_message(client_socket, message: bytes, message_type: int):
    header = struct.pack('!IB', len(message), message_type)

    client_socket.sendall(header + message)


def send_file(client_socket, file_path):
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    file_name = file_name.encode(ENCODING_FORMAT)

    header = struct.pack(HEADER_FORMAT, file_size, len(file_name))
    client_socket.sendall(header + file_name)

    with open(file_path, 'rb') as file:
        while True:
            chunk = file.read(4096)
            if not chunk:
                break

            client_socket.sendall(chunk)


def main():
    client_socket = connect_to_host(HOST)
    while True:
        msg = input('Command: ')
        if msg[0:5] == '/send':
            path = msg[6:]
            if not os.path.exists(path):
                print(f'File "{path}" does not exist')

                continue

            send_file(client_socket=client_socket, file_path='client1/' + path)
            print(f'File "{path}" sent')
        else:
            send_message(client_socket=client_socket, message=msg.encode(ENCODING_FORMAT), message_type=1)


if __name__ == '__main__':
    main()
