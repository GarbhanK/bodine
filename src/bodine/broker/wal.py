"""
This is where the Write-Ahead log implementation will go

This WAL will be the database for the broker nodes.

Messages are stored as flat files on disk, one per-partition. Each partition is a directory of segment files (append-only binary files where messages are written to).

Writes always go to the end of the log, and reads are always forward from some offset. Messages are never updated or deleted by one ID, never queried by content. Having a full database when we only use a small subset of the typical feature-set is overkill for this application.

Every 'produce' call appends a record to the file and optionally calls `fsync` to flush it to disk before acknowledging the producer.

Each record in the log is encoded the same way as the TCP messages, the file is just a sequence of those frames. Reading by offset requires scanning the whole file sequentially (slow) or maintaining a separate in-memory mapping offset numbers to byte positions in the file. Kafka keeps an index like that in `.index` files alongside `.log` segment files. For the first implementation we can just scan then worry about indexing later.


"""

import threading
from dataclasses import dataclass, field
from pathlib import Path

from bodine.broker.models import Message


@dataclass
class Topic:
    name: str
    length: int = 0
    messages: list[Message] = field(default_factory=list)


@dataclass
class PIGDB:
    _topics: dict[str, Topic] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def insert(self, message: Message) -> None:
        with self._lock:
            if message.topic not in self._topics:
                self._topics[message.topic] = Topic(name=message.topic)

            # append message to the topic's messages list (soon to be WAL)
            self._topics[message.topic].messages.append(message)

            # update the topic's length
            self._topics[message.topic].length += 1

    def get(self, topic: str, offset: int) -> Message:
        with self._lock:
            # TODO: Re-think the logic for handling invalid topics and offsets
            # for now we create the topic if it doesn't exist
            if topic not in self._topics:
                self._topics[topic] = Topic(name=topic)

            topic_data: Topic = self._topics[topic]

            return topic_data.messages[offset]

    def get_all(self, topic: str) -> list[Message]:
        with self._lock:
            return self._topics[topic].messages

    def get_topic_size(self, topic: str) -> int:
        with self._lock:
            if topic not in self._topics:
                return 0
            return self._topics[topic].length


class WAL:
    def __init__(self, path: Path) -> None:
        self.path = path

        if not self.path.exists():
            self.path.mkdir(parents=True)

    def append(self, data: bytes) -> int:
        """Add data to the log and return the offset of the new record"""
        raise NotImplementedError()
