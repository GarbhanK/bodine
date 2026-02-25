import datetime as dt
import socket
import threading
import uuid
from dataclasses import dataclass, field


@dataclass
class Client:
    id: str = ""
    conn: socket.socket = field(default_factory=socket.socket)
    alive: bool = True
    group: str | None = None

    def __post_init__(self):
        self.id = str(uuid.uuid4())


"""
sample = {
    "name": "group1",
    "topics": {
        "default": {"<client_id1>": 0, "<client_id2>": 0},
        "greetings": {"<client_id1>": 0, "<client_id2>": 0},
    },
}
"""


@dataclass
class ConsumerGroup:
    name: str
    topics: dict[str, dict[str, int]] = field(default_factory=dict)

    def __post_init__(self):
        self.topics = {topic: {} for topic in self.topics}

    def get_offset(self, topic: str, client_id: str) -> int:
        return self.topics[topic].get(client_id, 0)


@dataclass
class ConsumerGroupRegistry:
    _groups: dict[str, ConsumerGroup] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def _seed_groups(self, consumer_groups: list[str]):
        with self._lock:
            for group_name in consumer_groups:
                self._groups[group_name] = ConsumerGroup(name=group_name)

    def add_consumer(self, group_name: str, topic: str, client_id: str) -> None:
        with self._lock:
            if self._groups[group_name].topics.get(topic):
                self._groups[group_name].topics[topic][client_id] = 0
                print(f"DEBUG! added consumer to {group_name}/{topic}: {client_id}")

    def add_group(self, group_name: str, topics: list[str]) -> None:
        with self._lock:
            self._groups[group_name] = ConsumerGroup(name=group_name)

    def add_topic(self, group_name: str, topic: str) -> None:
        with self._lock:
            if self._groups[group_name].topics.get(topic):
                raise ValueError(
                    f"Topic '{topic}' already exists for group '{group_name}'"
                )
            self._groups[group_name].topics[topic] = {}

    def get_topics(self, group_name: str) -> list[str]:
        with self._lock:
            return list(self._groups[group_name].topics.keys())

    def lookup(self, group_name: str, topic: str, client_id: str) -> int:
        """Returns the corresponding offset for a group"""

        with self._lock:
            group_info: ConsumerGroup | None = self._groups.get(group_name)
            if not group_info:
                raise ValueError(
                    f"Subscriber group {group_name} not found in the registry"
                )

            offset: int = group_info.get_offset(topic, client_id)
            return offset

    def increment(self, group_name: str, topic: str, client_id: str) -> None:
        """Increments the offset for a group"""
        with self._lock:
            group_info: ConsumerGroup | None = self._groups.get(group_name)
            if not group_info:
                raise ValueError(
                    f"Subscriber group {group_name} not found in the registry"
                )
            group_info.topics[topic][client_id] += 1


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
            print(f"Client {client_id} removed")

    def all_clients(self) -> list[Client]:
        with self._lock:
            return list(self._clients.values())


@dataclass
class Message:
    event: str
    content: str
    topic: str
    group: str

    timestamp: float = field(default_factory=dt.datetime.now().timestamp)
    sent: bool = field(default=False)

    def __post_init__(self):
        if not self.event:
            raise ValueError("Event cannot be empty")
        if not self.topic:
            raise ValueError("Topic cannot be empty")
