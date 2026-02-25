import json
import socket
import struct
import time
from typing import Generator

from bodine.client.base import ClientBase


class Subscriber(ClientBase):
    def __init__(self, broker, topic):
        address, port = broker.split(":")
        self.address = address
        self.port = int(port)
        self.topic = topic
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._connect()

    def __repr__(self):
        return f"Subscriber(address='{self.address}', port={self.port}, topic='{self.topic}')"

    def _connect(self):
        print(f"Connecting to {self.address}:{self.port}...")
        self.sock.connect((self.address, self.port))

        print("Sending initial connection request...")
        payload: str = json.dumps(
            {
                "event": "subscribe",
                "topic": self.topic,
                "group": "default",
                "content": "",
            }
        )
        connection_message: bytes = self._build_payload(payload)
        self.sock.sendall(connection_message)

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

    def _recv_message(self, sock: socket.socket) -> dict:
        """Decode a message received from a client."""
        # length header is exactly 4 bytes
        raw_length: bytes = self._recv_exactly(sock, 4)

        # unpack big-endian unsigned int from the length header
        length: int = struct.unpack(">I", raw_length)[0]

        # Step 2: now read exactly that many bytes
        raw_payload: bytes = self._recv_exactly(sock, length)

        try:
            message_content: dict = json.loads(raw_payload.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON payload: {e}")

        return message_content

    def _fetch_next(self) -> dict:
        """Fetches the next message from the broker. Send polling message to the broker"""

        payload: str = json.dumps({"event": "poll", "topic": self.topic, "content": ""})
        polling_message = self._build_payload(payload)
        self.sock.sendall(polling_message)
        print(f"Sent polling message: {payload}")

        print("Waiting for response...")
        response: dict = self._recv_message(self.sock)

        return response

    def poll(self, default_interval=3.0) -> Generator[dict]:
        print(f"Starting consumer for topic {self.topic} at {self.address}:{self.port}")

        interval: float = default_interval

        while True:
            message: dict = self._fetch_next()
            if message.get("status") == "NO_NEW_MESSAGES":
                interval = interval * 2
                if interval > default_interval:
                    interval = default_interval

                print(f"No new messages available, sleeping for {interval} seconds")
                time.sleep(interval)
            else:
                interval = 0.2
                yield message
