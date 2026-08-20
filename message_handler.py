import json
import time
from protocol import MessageType


def process_message(connection, header, payload):
    message_type = header['type']
    stream_id = header['stream_id']

    if message_type == MessageType.PING:
        handle_ping(connection, payload)
    elif message_type == MessageType.PONG:
        handle_pong(connection, payload)

        return f'{connection.rtt_ms} ms'
    elif message_type == MessageType.TEXT:
        return payload.decode('utf-8')

    return None


def handle_ping(connection, payload):
    metadata = json.loads(payload.decode('utf-8'))
    metadata['ping_received_time'] = time.perf_counter()

    connection.pong(metadata)


def handle_pong(connection, payload):
    metadata = json.loads(payload.decode('utf-8'))
    pong_received_time = time.perf_counter()

    total_rtt = pong_received_time - metadata['ping_sent_time']
    processing_time = metadata['pong_sent_time'] - metadata['ping_received_time']

    connection.rtt_ms = (total_rtt - processing_time) * 1000
