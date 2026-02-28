"""

Example of ConsumerGroupRegistry layout given...
groups = [group1, group2]
topics = [topic1, topic2]
n_partitions: 2

{
    "group1": {
        "topics": {
            "topic1": {
                0: 4,
                1: 3,
            },
            "topic2": {
                0: 0,
                1: 2,
            },
        },
    },
    "group2":
        "topics": {
            "topic1": {
                0: 2,
                1: 2,
            },
            "topic2": {
                0: 5,
                1: 3,
            },
        },
    },
}
"""

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
    partition: int = 0

    def __post_init__(self):
        self.id = str(uuid.uuid4())


@dataclass
class Event:
    type: str
    content: str
    topic: str
    group: str

    # TODO: implement timestamp generation logic
    timestamp: float = field(default_factory=dt.datetime.now().timestamp)

    def __post_init__(self):
        if not self.type:
            raise ValueError("Event cannot be empty")
        if not self.topic:
            raise ValueError("Topic cannot be empty")


@dataclass
class ConsumerGroupRegistry:
    """
    groups structure: dict[group_name, dict[topic, dict[partition, offset]]]
    """

    _registry: dict[str, dict[str, dict[int, int]]] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def setup(self, groups: list[str], wal_info: dict) -> None:
        """ """
        for group_name in groups:
            self._registry[group_name] = wal_info

    def add_consumer(self, group_name: str, topic: str, partition: int) -> None:
        with self._lock:
            if self._registry[group_name][topic].get(partition) is None:
                raise ValueError(
                    f"Partition {partition} does not exist for topic {topic}! (could be due to not implemented rebalancing logic?)"
                )

            # Get the consumer group's current offset for this partition and increment it by 1
            group_offset: int = self._registry[group_name][topic][partition]
            self._registry[group_name][topic][partition] = group_offset

            print(f"Added consumer to {group_name}/{topic}/{partition}@{group_offset}")

    def add_group(
        self, group_name: str, topics: list[str], num_partitions: int
    ) -> None:
        with self._lock:
            new_group = {}
            for topic in topics:
                new_group = {topic: {n: 0} for n in range(num_partitions)}

            self._registry[group_name] = new_group
            print(f"Added group: {new_group}")

    def add_topic(self, group_name: str, topic: str, num_partitions: int) -> None:
        with self._lock:
            if self._registry[group_name].get(topic):
                raise ValueError(
                    f"Topic '{topic}' already exists for group '{group_name}'"
                )
            self._registry[group_name][topic] = {n: 0 for n in range(num_partitions)}

    def get_topics(self, group_name: str) -> list[str]:
        with self._lock:
            return list(self._registry[group_name].keys())

    def lookup(self, group_name: str, topic: str, partition: int) -> int:
        """Returns the corresponding offset for a group"""

        with self._lock:
            if not self._registry.get(group_name):
                raise ValueError(
                    f"Subscriber group {group_name} not found in the registry"
                )

            try:
                offset: int = self._registry[group_name][topic][partition]
            except KeyError as e:
                raise ValueError(
                    f"Unable to find offset under {group_name}/{topic}/{partition}: {e}"
                ) from e
            return offset

    def increment(self, group_name: str, topic: str, partition: int) -> None:
        """Increments the offset for a group"""
        with self._lock:
            if not self._registry.get(group_name):
                raise ValueError(
                    f"Subscriber group {group_name} not found in the registry"
                )

            self._registry[group_name][topic][partition] += 1


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

    def rebalance_clients(self, num_partitions: int) -> None:
        with self._lock:
            # TODO: implement rebalancing logic
            raise NotImplementedError()
