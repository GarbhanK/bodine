import json
import socket

from bodine.client.base import ClientBase


class Publisher(ClientBase):
    def __init__(self, broker, topic):
        address, port = broker.split(":")
        self.address = address
        self.port = int(port)
        self.topic = topic
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._connect()

    def __repr__(self) -> str:
        return f"Publisher({self.address}:{self.port}, {self.topic})"

    def _connect(self):
        print(f"Connecting to {self.address}:{self.port}...")
        self.sock.connect((self.address, self.port))

        print("Sending initial connection request...")
        payload: str = json.dumps(
            {
                "topic": self.topic,
                "event": "publish",
                "content": "",
            }
        )
        connection_message = self._build_payload(payload)
        self.sock.sendall(connection_message)

    def send(self, message: str) -> None:
        print(f"Sending message '{message}' to '{self.topic}'...")
        try:
            self.sock.sendall(self._build_payload(message))
        except Exception as e:
            print(f"Error sending message: {e}")
