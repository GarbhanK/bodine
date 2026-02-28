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
    # fd: int  # TODO: is this only set/lasts as long as file is open? Useful as id?
    end_offset: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self):
        if self.fpath.exists():
            # if file exists, set end offset to the partition size (so we don't replay all messages in the partition)
            logger.info(f"WAL file exists at {self.fpath}, recovering topic state...")
            self.end_offset = self._get_partition_size()
        else:
            # create file if not exists
            self.fpath.touch()
            logger.info(f"WAL file created at {self.fpath}!")

    def append(self, data: bytes) -> None:
        """Append raw bytes data to the log"""
        with self._lock:
            # 'append bytes' mode will create a file if none is present
            with open(self.fpath, mode="ab") as f:
                f.write(data)

    def read_from(self, offset: int) -> dict:
        with self._lock:
            # brainstorming
            # index x messages (length-encoded json) into the bytes
            # naive -> open file, iterate sequentially by reading length-encoding then jumping past content
            #          n_skipped is the offset, once we match input arg we read content bytes and return
            # This sounds really slow and un-scalable but it might be what index files fix, that's for later

            # file index for offset tracking, +1 for each complete header+message in the log
            file_idx: int = 0

            with open(self.fpath, "rb") as f:
                while file_idx < offset:
                    # get the length of the next message
                    raw_length: bytes = f.read(HEADER_SIZE)
                    length: int = struct.unpack(">I", raw_length)[0]

                    # skip the message content
                    f.seek(length, os.SEEK_CUR)

                    # increment file index
                    file_idx += 1

                # when we reach desired offset, read the message content
                if file_idx != offset:
                    raise ValueError(f"Offset {offset} is out of range")

                # read the message content
                target_message_len_raw: bytes = f.read(HEADER_SIZE)
                target_message_length: int = struct.unpack(
                    ">I", target_message_len_raw
                )[0]
                raw_target_payload: bytes = f.read(target_message_length)

                try:
                    message_content: dict = json.loads(
                        raw_target_payload.decode("utf-8")
                    )
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON payload: {e}")

                return message_content

    def _get_partition_size(self) -> int:
        """Get the size (max offset) of the partition"""
        file_idx: int = 0
        end_of_log: bool = False

        with open(self.fpath, "rb") as f:
            while not end_of_log:
                # get length of the next message
                raw_length: bytes = f.read(HEADER_SIZE)
                if not raw_length:
                    end_of_log = True
                    break

                # get event content length and skip the event content
                length: int = struct.unpack(">I", raw_length)[0]
                f.seek(length, os.SEEK_CUR)

                # increment file index for each event
                file_idx += 1

        logger.info(f"Partition size: {file_idx}")
        return file_idx


# TODO: redundant?
@dataclass
class Topic:
    name: str
    partitions: dict[int, WAL] = field(default_factory=dict)


@dataclass
class Storage:
    n_partitions: int
    location: str

    _topics: dict[str, Topic] = field(default_factory=dict)

    def setup(self, topics: list[str]) -> None:
        for topic in topics:
            self._topics[topic] = Topic(name=topic)

            for i in range(self.n_partitions):
                wal_path: str = f"{topic}-{i}.wal"
                self._topics[topic].partitions[i] = WAL(
                    topic=topic, partition=i, fpath=Path(wal_path)
                )

    def insert(self, topic: str, partition: int, message: bytes) -> None:
        # add message to the end of the WAL
        self._topics[topic].partitions[partition].append(message)

        # update the end offset (i.e length) of the partition
        self._topics[topic].partitions[partition].end_offset += 1

    def get(self, topic: str, partition: int, offset: int) -> dict:
        return self._topics[topic].partitions[partition].read_from(offset)

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
