import socket


class Producer:
    def __init__(self, broker, topic):
        address, port = broker.split(":")
        self.address = address
        self.port = int(port)
        self.topic = topic
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._connect()

    def __repr__(self) -> str:
        return f"Producer({self.address}:{self.port}, {self.topic})"

    def _connect(self):
        print(f"Connecting to {self.address}:{self.port}...")
        self.sock.connect((self.address, self.port))

    def _build_payload(self, message: str) -> bytes:
        """Create a message with the topic and message. The message length is added to the first 4 bytes of the payload"""
        length = len(message)
        length_header: bytes = length.to_bytes(4, byteorder="big")
        return length_header + message.encode(encoding="utf-8")

    def send(self, message: str) -> None:
        print(f"Sending message '{message}' to topic '{self.topic}'...")
        try:
            self.sock.sendall(self._build_payload(message))
        except Exception as e:
            print(f"Error sending message: {e}")
