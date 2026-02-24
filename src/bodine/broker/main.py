import json
import socket
import struct
import sys
import threading
from typing import Callable

# from collections import deque
from bodine.broker.models import PIG_DB, Client, ConnRegistry, Message
from bodine.broker.utils import (
    ClientDisconnected,
    # ShutdownException,
)

PORT: int = 9001
MAX_CONNECTIONS: int = 5

PIGDB = PIG_DB()
CLIENTS = ConnRegistry()


def main() -> None:
    print("Starting broker...")

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as e:
        print(f"Socket creation failed with error {e}")
        sys.exit(1)

    # bind to local network and specified port
    s.bind(("127.0.0.1", PORT))
    print(f"socket binded to {PORT}")

    # put socket into listening mode
    s.listen(MAX_CONNECTIONS)
    print("Socket is listening...")

    try:
        while True:
            conn, addr = s.accept()
            print(f"Got connection from {addr}")
            spawn_worker_thread(conn, handle_connection)

    except (KeyboardInterrupt, SystemExit):
        print("Shutting down...")
        s.close()


def spawn_worker_thread(conn: socket.socket, func: Callable):
    """Spawn a worker thread to handle messages from a client."""
    thread = threading.Thread(target=func, args=(conn, PIGDB))
    thread.start()


def recv_message(sock: socket.socket) -> Message | None:
    """Decode a message received from a client."""
    try:
        # length header is exactly 4 bytes
        raw_length: bytes = recv_exactly(sock, 4)

        # unpack big-endian unsigned int from the length header
        length: int = struct.unpack(">I", raw_length)[0]

        # Step 2: now read exactly that many bytes
        raw_payload: bytes = recv_exactly(sock, length)
    except ClientDisconnected:
        return None

    try:
        message_content: dict = json.loads(raw_payload.decode("utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON payload: {e}")

    try:
        topic: str = message_content["topic"].strip()
        content: str = message_content["message"].strip()
        action: str = message_content["action"].strip()
    except KeyError as e:
        raise ValueError(f"Invalid message format: {message_content}") from e

    return Message(topic=topic.upper(), payload=content, action=action)


def recv_exactly(sock, n: int) -> bytes:
    """Returns the exact amout of n bytes from the socket."""
    buf = b""

    # the while loop logic handles the case where the socket is closed mid-message
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ClientDisconnected("Socket closed mid-message")
        buf += chunk
    return buf


def handle_connection(conn: socket.socket, db: PIG_DB) -> None:
    """Listen for incoming connections on the specified port."""

    # add it to the client connection
    client: Client = Client(conn=conn)
    CLIENTS.add_client(client)

    # handle client connection

    first_message: Message | None = recv_message(sock=client.conn)
    print(f"Connection message: {first_message}")

    if first_message is None:
        raise ValueError("Invalid connection message")

    match first_message.action:
        case "subscribe":
            handle_subscriber(client, topic=first_message.topic)
        case "publish":
            handle_publisher(client, topic=first_message.topic)
        case _:
            raise ValueError(f"Unsupported action: {first_message.action}")

    # client socket disconnect
    CLIENTS.remove_client(client.id)
    conn.close()


def handle_publisher(client: Client, topic: str) -> None:
    """Publish a message to the topic"""

    print(f"Handling publisher client: {client.id}")
    while client.alive:
        message: Message | None = recv_message(client.conn)
        if message is None:
            client.alive = False
            continue

        print(f"Received message: {message}")

        # insert message into the database
        PIGDB.insert(message)


def handle_subscriber(client: Client, topic: str) -> None:
    """Respond to the client with the offset message from the topic"""

    print(f"Handling subscriber client: {client.id} ({topic})")
    while client.alive:
        message: Message | None = recv_message(client.conn)
        if message is None:
            client.alive = False
            continue

        print(f"{topic}@{client.id} Poll recieved...")

        # TODO: offset tracking should be per-subscriber-group, not coupled to the client id
        # get client's offset from the database

        # create a new offset if topic doesn't exist
        if topic not in client.offsets:
            client.offsets[topic] = 0

        topic_offset: int = client.offsets[topic]
        topic_size: int = PIGDB.get_topic_size(topic)

        # Raise an error if the offset is greater than the topic size
        if topic_offset > topic_size:
            raise ValueError(
                f"Invalid Offset: offset {topic_offset} is greater than topic size {topic_size}"
            )

        # if subscriber is at the end of the topic, tell them to wait for new messages
        print(f"{topic_offset=}, {topic_size=}")
        if topic_offset == topic_size:
            resp = Message(action="NO_NEW_MESSAGES", topic=topic, payload="")
            send_poll_response(sock=client.conn, status=resp.action, message=resp)
            continue

        # retrieve message from the database matching the offset
        offset_message: Message = PIGDB.get(topic, topic_offset)

        # increment client offset by 1
        client.offsets[topic] += 1

        # respond to the client with the offset message
        send_poll_response(sock=client.conn, status="OK", message=offset_message)


def _build_payload(message: str) -> bytes:
    """Create a message with the topic and message. The message length is added to the first 4 bytes of the payload"""
    length = len(message)
    length_header: bytes = length.to_bytes(4, byteorder="big")
    return length_header + message.encode(encoding="utf-8")


def send_poll_response(sock: socket.socket, status: str, message: Message) -> None:
    """Send a message to the client"""
    payload: str = json.dumps({"status": status, "message": message.payload})
    print(f"Sending response: {payload}")
    full_response: bytes = _build_payload(payload)
    sock.sendall(full_response)


if __name__ == "__main__":
    main()
