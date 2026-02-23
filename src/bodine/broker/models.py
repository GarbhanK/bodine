import datetime as dt
import socket
import threading
import uuid
from dataclasses import dataclass, field


@dataclass
class Client:
    id: str = ""
    conn: socket.socket = field(default_factory=socket.socket)
    group: str | None = None
    offsets: dict[str, int] = field(default_factory=dict)  # topic -> offset

    def __post_init__(self):
        self.id = str(uuid.uuid4())


@dataclass
class ConnRegistry:
    _clients: dict[str, Client] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add_client(self, client: Client) -> None:
        with self._lock:
            self._clients[client.id] = client
            print(f"Client {client.id} added")

    def remove_client(self, client_id: str) -> None:
        with self._lock:
            self._clients.pop(client_id, None)

    def all_clients(self) -> list[Client]:
        with self._lock:
            return list(self._clients.values())


@dataclass
class Message:
    action: str
    topic: str
    payload: str
    timestamp: float = field(default_factory=dt.datetime.now().timestamp)
    sent: bool = field(default=False)


@dataclass
class Topic:
    name: str
    messages: list[Message] = field(default_factory=list)


@dataclass
class PIG_DB:
    _topics: dict[str, Topic] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def insert(self, message: Message) -> None:
        with self._lock:
            if message.topic not in self._topics:
                self._topics[message.topic] = Topic(name=message.topic)

            self._topics[message.topic].messages.append(message)

    def get(self, topic: str, offset: int) -> Message:
        with self._lock:
            return self._topics[topic].messages[offset]

    def get_all(self, topic: str) -> list[Message]:
        with self._lock:
            return self._topics[topic].messages
