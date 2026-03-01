"""
This is where the Write-Ahead log implementation will go

This WAL will be the database for the broker nodes.

Messages are stored as flat files on disk, one per-partition. Each partition is a directory of segment files (append-only binary files where messages are written to).

Writes always go to the end of the log, and reads are always forward from some offset. Messages are never updated or deleted by one ID, never queried by content. Having a full database when we only use a small subset of the typical feature-set is overkill for this application.

Every 'produce' call appends a record to the file and optionally calls `fsync` to flush it to disk before acknowledging the producer.

Each record in the log is encoded the same way as the TCP messages, the file is just a sequence of those frames. Reading by offset requires scanning the whole file sequentially (slow) or maintaining a separate in-memory mapping offset numbers to byte positions in the file. Kafka keeps an index like that in `.index` files alongside `.log` segment files. For the first implementation we can just scan then worry about indexing later.

"""

import json
import os
import struct
import threading
from dataclasses import dataclass, field
from pathlib import Path

from bodine.broker.logs import get_logger
from bodine.broker.utils import HEADER_SIZE

logger = get_logger(__name__)


@dataclass
class WAL:
    topic: str
    partition: int
    fpath: Path
    end_offset: int = 0
    fd: int = -1
    offsets: dict[int, int] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self):
        if self.fpath.exists():
            # if file exists, set end offset to the partition size (so we don't replay all messages in the partition)
            logger.info(f"WAL file exists at {self.fpath}, recovering topic state...")
            logical_offset, byte_offset = self._get_partition_size()
            self.end_offset = logical_offset
        else:
            # create file if not exists
            self.fpath.touch()
            logger.info(f"WAL file created at {self.fpath}!")

        # double check os.O_RDWR is the right flag. Maybe os.O_APPEND or os.O_FSYNC
        self.fd = os.open(self.fpath, os.O_RDWR)
        if self.fd < 0:
            raise OSError(f"Failed to open WAL file {self.fpath}")

    def append(self, data: bytes) -> None:
        """Append raw bytes data to the log file"""
        with self._lock:
            # write to file descriptor
            os.write(self.fd, data)

            # increment end offset
            self.end_offset += 1

    def read_from(self, logical_offset: int) -> dict:
        with self._lock:
            byte_offset: int = self.offsets[logical_offset]

            # seek to the byte offset in the file
            os.lseek(self.fd, byte_offset, os.SEEK_SET)

            # read the message content
            target_message_len_raw: bytes = os.read(self.fd, HEADER_SIZE)
            target_message_length: int = struct.unpack(">I", target_message_len_raw)[0]
            raw_target_payload: bytes = os.read(self.fd, target_message_length)

            # reset to start of the file
            os.lseek(self.fd, 0, os.SEEK_SET)

            try:
                message_content: dict = json.loads(raw_target_payload.decode("utf-8"))
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON payload: {e}")

            return message_content

    def _get_partition_size(self) -> tuple[int, int]:
        """Get the size (max offset) of the partition

        Returns:
            dict[int, int]: A dictionary mapping logical offset to the byte offset.
        """
        original_fd_pos = os.lseek(
            self.fd, 0, os.SEEK_CUR
        )  # save current file pointer position
        os.lseek(self.fd, 0, os.SEEK_SET)  # seek to beginning of the file

        logical_idx: int = 0
        byte_idx: int = 0

        # TODO: logic for recovering half-written messages
        while True:
            # get length of the next message (os.read advances the file pointer)
            raw_length: bytes = os.read(self.fd, HEADER_SIZE)
            if not raw_length:
                break

            # get event content length and skip the event content
            content_length: int = struct.unpack(">I", raw_length)[0]

            # seek forward to skip the event content
            os.lseek(self.fd, content_length, os.SEEK_CUR)

            # increment logical index and byte index for each event
            logical_idx += 1
            byte_idx += HEADER_SIZE + content_length

        os.lseek(
            self.fd, original_fd_pos, os.SEEK_SET
        )  # restore original file pointer position

        logger.info(f"Partition size: {logical_idx} ({byte_idx} bytes)")
        return logical_idx, byte_idx


# TODO: redundant?
@dataclass
class Topic:
    name: str
    partitions: dict[int, WAL] = field(default_factory=dict)


@dataclass
class Storage:
    n_partitions: int
    location: str

    _next_partition: int = 0
    _topics: dict[str, Topic] = field(default_factory=dict)

    def setup(self, topics: list[str]) -> None:
        wal_dir = Path(self.location)

        if not wal_dir.exists():
            wal_dir.mkdir(parents=True)
            logger.info(f"Created WAL directory: {wal_dir}")

        for topic in topics:
            self._topics[topic] = Topic(name=topic)

            for i in range(self.n_partitions):
                wal_path: Path = Path(self.location) / f"{topic}-{i}.wal"
                self._topics[topic].partitions[i] = WAL(
                    topic=topic, partition=i, fpath=wal_path
                )

    def insert(self, topic: str, partition: int, message: bytes) -> None:
        """Append a message to the WAL"""
        self._topics[topic].partitions[partition].append(message)

    def get(self, topic: str, partition: int, logical_offset: int) -> dict:
        return self._topics[topic].partitions[partition].read_from(logical_offset)

    def get_partition_size(self, topic: str, partition: int) -> int:
        return self._topics[topic].partitions[partition].end_offset

    def get_wal_info(self) -> dict:
        """Return a dict of group, topic, partiton and offset data
        to set up the ConsumerRegistry. The consumer registry is seeded the same
        across different groups, but the offsets are changed and tracked differently
        as the broker processes events.
        """

        info = {}
        for topic in self._topics:
            info[topic] = {
                partition: wal.end_offset
                for partition, wal in self._topics[topic].partitions.items()
            }
        return info


# NOTE: this was the old in-memory data store used when developing socket/TCP code.
#       could still be useed for testing/debugging purposes. leaving it here for now.
#
# @dataclass
# class PIGDB:
#     _topics: dict[str, Topic] = field(default_factory=dict)
#     _lock: threading.Lock = field(default_factory=threading.Lock)

#     def insert(self, message: Message) -> None:
#         with self._lock:
#             if message.topic not in self._topics:
#                 self._topics[message.topic] = Topic(name=message.topic)

#             # append message to the topic's messages list (soon to be WAL)
#             self._topics[message.topic].messages.append(message)

#             # update the topic's length
#             self._topics[message.topic].length += 1

#     def get(self, topic: str, offset: int) -> Message:
#         with self._lock:
#             # TODO: Re-think the logic for handling invalid topics and offsets
#             # for now we create the topic if it doesn't exist
#             if topic not in self._topics:
#                 self._topics[topic] = Topic(name=topic)

#             topic_data: Topic = self._topics[topic]

#             return topic_data.messages[offset]

#     def get_all(self, topic: str) -> list[Message]:
#         with self._lock:
#             return self._topics[topic].messages

#     def get_topic_size(self, topic: str) -> int:
#         with self._lock:
#             if topic not in self._topics:
#                 return 0
#             return self._topics[topic].length
