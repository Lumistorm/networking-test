import socket
import time
import struct


HOST = '192.168.18.44'
PORT = 8000
HEADER_SIZE = 5
HEADER_FORMAT = '!IB'
ENCODING_FORMAT = 'utf-8'


def connect_to_host(host_name: str):
    address = (host_name, PORT)

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(address)

    return client


def send_message(client, message: str, message_type: int):
    message = message.encode(ENCODING_FORMAT)
    header = struct.pack(HEADER_FORMAT, len(message), message_type)

    client.sendall(header + message)


def main():
    client = connect_to_host(HOST)
    while True:
        a = input('Input: ')
        send_message(client=client, message=a, message_type=0)


if __name__ == '__main__':
    main()
