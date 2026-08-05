import socket
import threading
import struct


HOST = '192.168.18.44'
PORT = 8000
HEADER_SIZE = 5
HEADER_FORMAT = '!IB'


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
    connected = True

    while connected:
        message = receive_message(client_socket)

        if message:
            message = message.decode('utf-8')
            print(f'{client_address}: {message}')

            if message == '/quit':
                connected = False

    client_socket.close()


def receive_message(client_socket):
    header = client_socket.recv(HEADER_SIZE)
    if len(header) < HEADER_SIZE:
        return None

    message_length, message_type = struct.unpack(HEADER_FORMAT, header)

    message = b''
    while len(message) < message_length:
        packet = client_socket.recv(message_length)
        if not packet:
            return None
        message += packet

    return message


def main():
    server = create_server()
    start_server(server)


if __name__ == '__main__':
    main()
