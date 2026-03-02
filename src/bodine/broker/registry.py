import threading
from dataclasses import dataclass, field

from bodine.broker.logs import get_logger

logger = get_logger()


@dataclass
class ConsumerGroup:
    """Represents a consumer group in the registry.

    assignments:
        topic -> client_id -> list[partitions]
    offsets:
        topic -> partition -> offset
    """

    name: str
    members: list[str] = field(default_factory=list)  # all client ids
    # TODO: stores client->parition mappings made during rebalancing
    assignments: dict[str, dict[str, list[int]]] = field(default_factory=dict)
    offsets: dict[tuple[str, int], int] = field(default_factory=dict)

    def get_offsets(self, topic: str, partitions: list[int]) -> dict[int, int]:
        offsets: dict[int, int] = {
            partition: self.offsets[(topic, partition)] for partition in partitions
        }
        return offsets

    def get_client_partitions(self, client_id: str, topic: str) -> list[int]:
        return self.assignments[topic][client_id]


class ConsumerGroupRegistry:
    """Registry for Consumer Groups. Each consumer group tracks partition->offset mappings
    for each topic.

    group_name -> ConsumerGroup -> topic_name -> partition -> offset

    Registry data example...
    group1->
        topic1->
            p0->5
            p1->3
        topic2->
            p0->5
            p1->3
    group2->
        topic1->
            p0->1
            p1->0
        topic2->
            p0->3
            p1->3
    etc...

    """

    _registry: dict[str, ConsumerGroup] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def __repr__(self) -> str:
        return f"ConsumerGroupRegistry({self._registry})"

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
            self._registry[group_name].assignments[topic][client_id].append(partition)

            logger.info(
                f"Added consumer {client_id} to {group_name}/{topic}/{partition}"
            )

    def get_topics(self, group_name: str) -> list[str]:
        with self._lock:
            return list(self._registry[group_name].assignments.keys())

    def get_partitions(self, client_id: str, topic: str, group_name: str) -> list[int]:
        with self._lock:
            partitions: list[int] = self._registry[group_name].get_client_partitions(
                client_id, topic
            )
            return partitions

    def lookup(self, client_id: str, group_name: str, topic: str) -> dict[int, int]:
        """Returns the corresponding offsets for the client in a consumer group"""
        with self._lock:
            cgroup: ConsumerGroup | None = self._registry.get(group_name)
            if not cgroup:
                raise ValueError(
                    f"Subscriber group {group_name} not found in the registry"
                )

            client_partitions: list[int] = cgroup.get_client_partitions(
                client_id=client_id, topic=topic
            )

            offset: dict[int, int] = cgroup.get_offsets(topic, client_partitions)
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
