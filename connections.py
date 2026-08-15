import socket


def create_tcp_socket(host, port):
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    tcp_socket.bind((host, port))
    tcp_socket.listen()

    return tcp_socket


def create_udp_socket(host, port):
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    udp_socket.bind((host, port))

    return udp_socket


def connect(host, port, timeout):
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.settimeout(timeout)

    try:
        connection.connect((host, port))
    except OSError:
        connection.close()
        raise

    connection.settimeout(None)

    return connection


def close_connection(connection):
    try:
        connection.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass

    connection.close()
