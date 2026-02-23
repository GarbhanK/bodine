import socket


class Consumer:
    def __init__(self, broker, topic):
        address, port = broker.split(":")
        self.address = address
        self.port = int(port)
        self.topic = topic
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._connect()

    def _connect(self):
        print(f"Connecting to {self.address}:{self.port}...")
        self.socket.connect((self.address, self.port))

    def __repr__(self):
        return f"Consumer(address='{self.address}', port={self.port}, topic='{self.topic}')"

    def poll(self):
        print(f"Starting consumer for topic {self.topic} at {self.address}:{self.port}")
        raise NotImplementedError()
