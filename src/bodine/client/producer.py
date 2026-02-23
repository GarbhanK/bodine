import socket


class Producer:
    def __init__(self, broker, topic):
        address, port = broker.split(":")
        self.address = address
        self.port = int(port)
        self.topic = topic
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._connect()

    def __repr__(self) -> str:
        return f"Producer({self.address}:{self.port}, {self.topic})"

    def _connect(self):
        print(f"Connecting to {self.address}:{self.port}...")
        self.socket.connect((self.address, self.port))

    def send(self, message: str) -> None:
        print(
            f"Sending message '{message}' to topic '{self.topic}' via broker '{self.address}:{self.port}'"
        )
        try:
            self.socket.sendall(message.encode())
        except Exception as e:
            print(f"Error sending message: {e}")
