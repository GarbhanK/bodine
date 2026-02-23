import json
import socket
import struct
import sys

# from collections import deque
from bodine.broker.models import PIG_DB, Client, ConnRegistry, Message
from bodine.broker.utils import ShutdownException

PORT: int = 9001
MAX_CONNECTIONS: int = 5

PIGDB = PIG_DB()
CLIENTS = ConnRegistry()


def main() -> None:
    print("Starting broker...")
    broker_active: bool = True

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

    while broker_active:
        try:
            handle_messages(s, PIGDB)
        except ShutdownException:
            print("Shutdown requested")
            broker_active = False

    print("Shutting down...")


def recv_message(sock: socket.socket) -> Message:
    """Decode a message received from a client.
    e.g 'GREETINGS::hello world' (topic=GREETINGS, content=hello world)
    """
    # length header is exactly 4 bytes
    raw_length = recv_exactly(sock, 4)

    # unpack big-endian unsigned int from the length header
    length = struct.unpack(">I", raw_length)[0]

    # Step 2: now read exactly that many bytes
    raw_payload = recv_exactly(sock, length)

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
            raise ConnectionError("Socket closed mid-message")
        buf += chunk
    return buf


def handle_messages(sock: socket.socket, db: PIG_DB) -> None:
    """Listen for incoming connections on the specified port."""
    # establish connnection with client
    conn, addr = sock.accept()
    print(f"Got connection from {addr}")

    # add it to the client connection
    client: Client = Client(conn=conn)
    CLIENTS.add_client(client)
    # print(f"Clients: {CLIENTS}")

    # handle client connection
    try:
        while True:
            message: Message = recv_message(sock=conn)

            if message.action == "produce":
                # insert message into database
                PIGDB.insert(message)
            elif message.action == "consume":
                handle_consume(client, topic=message.topic)

            if message.payload == "SHUTDOWN":
                raise ShutdownException("Shutdown requested")

    except (ConnectionError, ConnectionResetError) as e:
        print(f"Connection error: {e}")
        pass
    finally:
        CLIENTS.remove_client(client.id)
        conn.close()


def handle_consume(client: Client, topic: str) -> None:
    """Respond to the client with the offset message from the topic"""

    topic_offset: int = client.offsets.get(topic, 0)

    # retrieve message from the database matching the offset
    offset_message: Message = PIGDB.get(topic, topic_offset)

    # increment client offset by 1
    client.offsets[topic] += 1

    send_message(sock=client.conn, message=offset_message)


def _build_payload(message: str) -> bytes:
    """Create a message with the topic and message. The message length is added to the first 4 bytes of the payload"""
    length = len(message)
    length_header: bytes = length.to_bytes(4, byteorder="big")
    return length_header + message.encode(encoding="utf-8")


def send_message(sock: socket.socket, message: Message) -> None:
    """Send a message to the client"""
    payload: str = json.dumps({"status": "OK", "message": message.payload})
    full_response: bytes = _build_payload(payload)
    sock.sendall(full_response)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Shutting down...")
        sys.exit(0)
