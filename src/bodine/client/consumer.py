import json
import socket
import struct
import time
from typing import Generator

# def recv_message(sock: socket.socket) -> Message:
#     """Decode a message received from a client.
#     e.g 'GREETINGS::hello world' (topic=GREETINGS, content=hello world)
#     """
#     # length header is exactly 4 bytes
#     raw_length = recv_exactly(sock, 4)

#     # unpack big-endian unsigned int from the length header
#     length = struct.unpack(">I", raw_length)[0]

#     # Step 2: now read exactly that many bytes
#     raw_payload = recv_exactly(sock, length)

#     try:
#         message_content: dict = json.loads(raw_payload.decode("utf-8"))
#     except json.JSONDecodeError as e:
#         raise ValueError(f"Invalid JSON payload: {e}")

#     try:
#         topic: str = message_content["topic"].strip()
#         content: str = message_content["content"].strip()
#         action: str = message_content["action"].strip()
#     except KeyError as e:
#         raise ValueError(f"Invalid message format: {message_content}") from e

#     return Message(topic=topic.upper(), payload=content, action=action)


class Consumer:
    def __init__(self, broker, topic):
        address, port = broker.split(":")
        self.address = address
        self.port = int(port)
        self.topic = topic
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._connect()

    def __repr__(self):
        return f"Consumer(address='{self.address}', port={self.port}, topic='{self.topic}')"

    def _connect(self):
        print(f"Connecting to {self.address}:{self.port}...")
        self.sock.connect((self.address, self.port))

    def _recv_exactly(self, sock, n: int) -> bytes:
        """Returns the exact amout of n bytes from the socket."""
        buf = b""

        # the while loop logic handles the case where the socket is closed mid-message
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Socket closed mid-message")
            buf += chunk
        return buf

    def _fetch_next(self) -> dict:
        raw_length = self._recv_exactly(self.sock, 4)

        # unpack big-endian unsigned int from the length header
        length = struct.unpack(">I", raw_length)[0]

        # Step 2: now read exactly that many bytes
        raw_payload = self._recv_exactly(self.sock, length)

        try:
            message_content: dict = json.loads(raw_payload.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON payload: {e}")

        print(f"Received message: {message_content}")
        return message_content

    def poll(self, interval=0.5) -> Generator[dict]:
        print(f"Starting consumer for topic {self.topic} at {self.address}:{self.port}")

        while True:
            message = self._fetch_next()
            if message:
                yield message
            else:
                print(f"No messages received, sleeping for {interval} seconds")
                time.sleep(interval)
