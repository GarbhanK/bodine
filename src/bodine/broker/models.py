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

# import datetime as dt
import socket
import threading
import uuid
from dataclasses import dataclass, field

from bodine.broker import logs

logger = logs.get_logger(__name__)


@dataclass
class Client:
    topic: str
    id: str = ""
    conn: socket.socket = field(default_factory=socket.socket)
    # partition: int | None = None
    # partitions: set[int] | None = (
    #     None  # TODO: Consumer (client) can be assigned to multiple partitions
    # )
    group: str | None = None
    alive: bool = True
    idle: bool = True

    def __post_init__(self):
        self.id = str(uuid.uuid4())


@dataclass
class Event:
    type: str
    content: str
    topic: str
    group: str
    key: str | None = None

    # TODO: implement timestamp generation logic
    # timestamp: float = field(default_factory=dt.datetime.now().timestamp)

    def __post_init__(self):
        if not self.type:
            raise ValueError("Event cannot be empty")
        if not self.topic:
            raise ValueError("Topic cannot be empty")


@dataclass
class ConsumerGroup:
    name: str
    members: list[str] = field(default_factory=list)  # all client ids

    # topic -> client_id -> partition
    assignments: dict[str, dict[str, int]] = field(default_factory=dict)

    # (topic, partition) -> commited offset
    offsets: dict[tuple[str, int], int] = field(default_factory=dict)

    def get_offset(self, topic: str, partition: int) -> int:
        return self.offsets[(topic, partition)]

    def get_client_partition(self, client_id: str, topic: str) -> int:
        return self.assignments[topic][client_id]


@dataclass
class ConsumerGroupRegistry:
    """
    groups structure: dict[group_name, dict[topic, dict[partition, offset]]]
    """

    # _registry: dict[str, dict[str, dict[int, int]]] = field(default_factory=dict)
    _registry: dict[str, ConsumerGroup] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def setup(self, groups: list[str], wal_info: dict[str, dict[int, int]]) -> None:
        """
        wal_info: dict[str, dict[int, int]]
            topic -> partition -> offset


        assignments are set later when rebalancing. This function just sets up the ConsumerGroup objs

        """
        logger.debug(f"{wal_info=}")
        topics: list[str] = list(wal_info.keys())

        for group_name in groups:
            self._registry[group_name] = ConsumerGroup(name=group_name)
            logger.debug(f"{topics=}")

            for topic_name in topics:
                # each topic maps cliend_ids->partition_no
                self._registry[group_name].assignments[topic_name] = {}

                partitions: dict[int, int] = wal_info[topic_name]

                # each offset (topic, partition)->offset
                for partition in partitions:
                    offset: int = wal_info[topic_name][partition]
                    self._registry[group_name].offsets[(topic_name, partition)] = offset

    def add_consumer(
        self, client_id: str, group_name: str, topic: str, partition: int
    ) -> None:
        """Add a new consumer to a consumer group. Specify the topic"""
        with self._lock:
            # make sure client_id is not already in the group
            group_ids: list[str] = [
                client_id
                for client_id in self._registry[group_name].assignments[topic].keys()
            ]
            if client_id in group_ids:
                raise ValueError("Client id already in this consumer group:")

            # add consumer to client registry
            # TODO: better to have this as None value by default, so we know what is missed by rebalancing?
            self._registry[group_name].assignments[topic][client_id] = partition

            logger.info(
                f"Added consumer {client_id} to {group_name}/{topic}/{partition}"
            )

    def get_topics(self, group_name: str) -> list[str]:
        with self._lock:
            return list(self._registry[group_name].assignments.keys())

    def get_partition(self, client_id: str, topic: str, group_name: str) -> int:
        with self._lock:
            partition: int = self._registry[group_name].get_client_partition(
                client_id, topic
            )
            return partition

    def lookup(self, client_id: str, group_name: str, topic: str) -> int:
        """Returns the corresponding offset for a group"""
        with self._lock:
            cgroup: ConsumerGroup | None = self._registry.get(group_name)
            if not cgroup:
                raise ValueError(
                    f"Subscriber group {group_name} not found in the registry"
                )

            client_partition: int = cgroup.get_client_partition(
                client_id=client_id, topic=topic
            )

            offset: int = cgroup.get_offset(topic, client_partition)
            return offset

    def increment(self, group_name: str, topic: str, partition: int) -> None:
        """Increments the offset for a group"""
        with self._lock:
            if not self._registry.get(group_name):
                raise ValueError(
                    f"Subscriber group {group_name} not found in the registry"
                )

            offset_idx: tuple[str, int] = (topic, partition)
            self._registry[group_name].offsets[offset_idx] += 1


@dataclass
class ConnRegistry:
    _clients: dict[str, Client] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add_client(self, client: Client) -> None:
        with self._lock:
            self._clients[client.id] = client
            logger.info(f"Client {client.id} added")

    def remove_client(self, client_id: str) -> None:
        with self._lock:
            self._clients.pop(client_id, None)
            logger.info(f"Client {client_id} removed")

    def all_clients(self) -> list[Client]:
        with self._lock:
            return list(self._clients.values())

    def rebalance_clients(self, num_partitions: int) -> None:
        """No need to thread lock because it is called upon accepting a connection, before
        we create handler thread.
        """
        logger.debug("NOT YET IMPLEMENTED")
        logger.debug(f"{self._clients=}")
        # p_count: int = 0
        # for client_id, client_info in self._clients.items():
        #     logger.debug(f"Rebalancing client {client_id}")
        #     if client_info.partition is None:
        #         # if all partitions have been assigned, mark client as idle and continue
        #         if p_count >= num_partitions:
        #             logger.info(f"Reached max partitions: {p_count}/{num_partitions}")
        #             client_info.idle = True
        #             continue

        #         logger.info(f"Assigning client {client_id} to partition {p_count}...")
        #         # assign to next available partition, set idle to False and increment to next available partition
        #         client_info.partition = p_count
        #         client_info.idle = False
        #         p_count += 1

        # logger.info(
        #     f"Rebalanced {len(self._clients)} clients to {num_partitions} partitions"
        # )
