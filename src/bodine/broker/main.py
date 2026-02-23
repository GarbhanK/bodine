import socket
import sys

# from collections import deque
from bodine.broker.models import Client, Message
from bodine.broker.utils import ShutdownException

PORT: int = 9000
MAX_CONNECTIONS: int = 5

PIGDB = {
    "topics": {
        "greetings": [],
        "signups": [],
    },
    "clients": [],
}


def main() -> None:
    print("Starting broker...")
    broker_active: bool = True

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as e:
        print(f"Socket creation failed with error {e}")
        sys.exit(1)

    # bind to local network and specified port
    s.bind(("", PORT))
    print(f"socket binded to {PORT}")

    # put socket into listening mode
    s.listen(MAX_CONNECTIONS)
    print("Socket is listening...")

    while broker_active:
        try:
            listen_for_messages(s, PIGDB)
        except ShutdownException:
            print("Shutdown requested")
            broker_active = False

        forward_messages(PIGDB)

    print("Shutting down...")


def decode_message(data: bytes) -> Message:
    """Decode a message received from a client.
    e.g 'GREETINGS::hello world' (topic=GREETINGS, content=hello world)
    """
    print("Decoding message...")

    message: str = data.decode().strip()
    print(f"Received data: {message}")

    message_parts: list[str] = message.split("::")
    if len(message_parts) != 2:
        raise ValueError(f"Invalid message format: {message}")

    topic = message_parts[0].strip()
    content = message_parts[1].strip()

    return Message(topic=topic.upper(), content=content)


def forward_messages(db: dict) -> None:
    """Forward messages from clients to other clients."""
    print("Forwarding messages...")

    clients: list[Client] = db["clients"]

    for client in clients:
        print(f"Forwarding messages to {client.address}:{client.port}")
        topic_messages = db["topics"][client.topic]
        for message in topic_messages:
            print(f"Sending message: {message}")
            client.conn.send(message.content.encode())
            message.sent = True


def listen_for_messages(sock: socket.socket, db: dict) -> None:
    """Listen for incoming connections on the specified port."""
    # establish connnection with client
    conn, addr = sock.accept()
    print(f"Got connection from {addr}")

    # todo: parse topic from client upon connection
    topic: str = "greetings"
    # topic: str = parse_topic()
    client: Client = Client(conn=conn, address=addr[0], port=addr[1], topic=topic)

    db["clients"].append(client)

    conn.send("Thank you for connecting\n".encode())

    # handle client connection
    while True:
        try:
            data: bytes = conn.recv(1024)
            print(repr(data))
            if not data:
                # client disconnected
                break

            message: Message = decode_message(data)

            # insert message into database
            db["topics"][message.topic].append(message)

            if message.content == "EXIT":
                break

            if message.content == "SHUTDOWN":
                raise ShutdownException("Shutdown requested")

        except ConnectionResetError:
            print("Client disconnected abruptly")
            break

    conn.close()


if __name__ == "__main__":
    main()
