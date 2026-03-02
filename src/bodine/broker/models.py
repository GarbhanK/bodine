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
        logger.debug("REBALANCE NOT YET IMPLEMENTED")
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
