import json
import socket
import struct
import threading
from dataclasses import dataclass, field
from typing import Any, Callable

from bodine.broker import logs
from bodine.broker.models import Client, ConnRegistry, ConsumerGroupRegistry, Message
from bodine.broker.utils import HEADER_SIZE, ClientDisconnected
from bodine.broker.wal import Storage

logger = logs.get_logger(__name__)


@dataclass(frozen=True)
class BrokerConfig:
    host: str
    port: int
    max_connections: int
    location: str
    partitions: int = 1
    topics: list[str] = field(default_factory=list)
    consumer_groups: list[str] = field(default_factory=list)


class Broker:
    host: str
    port: int
    max_connections: int
    sock: socket.socket
    partitions: int
    topics: list[str]
    active_clients: ConnRegistry
    consumer_registry: ConsumerGroupRegistry
    storage: Storage

    def __init__(self, cfg: BrokerConfig):
        self.host = cfg.host
        self.port = cfg.port
        self.max_connections = cfg.max_connections
        self.partitions = cfg.partitions
        self.topics = cfg.topics
        self.active_clients = ConnRegistry()
        self.consumer_registry = ConsumerGroupRegistry()
        self.storage = Storage(
            cfg.partitions,
            cfg.location,
        )

        # populate the consumer registry with the provided consumer groups
        self.consumer_registry.seed_groups(
            cfg.consumer_groups, cfg.topics, cfg.partitions
        )
        logger.info(self.consumer_registry)

        # setup storage and create partition WAL files for each topic
        self.storage.setup(cfg.topics)

    def __repr__(self) -> str:
        return f"Broker@{self.host}:{self.port}"

    def setup_listener(self) -> None:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as e:
            raise RuntimeError(f"Socket creation failed with error {e}")

        # bind to local network and specified port
        self.sock.bind((self.host, self.port))
        logger.info(f"socket binded to {self.port}")

        # put socket into listening mode
        self.sock.listen(self.max_connections)
        logger.info("Socket is listening...")

    def setup_consumer_groups(self) -> None:
        pass

    def accept_connections(self) -> None:
        while True:
            try:
                logger.info("Waiting to accept connection...")
                conn, addr = self.sock.accept()
                logger.info(f"Got connection from {addr}")
            except ConnectionAbortedError:
                logger.info("Connection aborted")
                continue  # ignore and keep listening
            except OSError as e:
                if e.errno == 9:  # bad file descriptor
                    logger.error("Bad file descriptor")
                    break  # break out of the loop
                logger.error(f"Accept failed: {e}")
                continue

            # add client to registry of active clients
            client: Client = Client(conn=conn)
            self.active_clients.add_client(client)

            # TODO: assign partition to the client and rebalance if needed

            # parse the connection message
            raw_conn_event: tuple[bytes, bytes] | None = self._recv_message(
                sock=client.conn
            )
            logger.info(f"Connection message: {raw_conn_event}")
            if raw_conn_event is None:
                raise ValueError("Invalid connection message")

            raw_header, raw_payload = raw_conn_event
            conn_event: Message = self._parse_message(raw_payload)

            thread_prefix: str = "S" if conn_event.event == "subscribe" else "P"
            thread_name: str = f"{thread_prefix}::{client.id}"
            self._spawn_worker_thread(
                name=thread_name,
                func=self.handle_connection,
                args=(client, conn_event),
            )

    # TODO: look into tracking threads as part of the broker class?
    def _spawn_worker_thread(self, name: str, func: Callable, args: tuple[Any, ...]):
        logger.info(f"Spawning thread '{name}'")
        thread = threading.Thread(name=name, target=func, args=args)
        thread.start()

    def handle_connection(self, client: Client, connection_message: Message) -> None:
        """Listen for incoming connections on the specified port."""

        # setup initial subscriber setup before handling socket stream in a loop
        if connection_message.event == "subscribe":
            if not connection_message.group:
                raise ValueError("Consumer group is required")

            # add client to the consumer registry
            self.consumer_registry.add_consumer(
                group_name=connection_message.group,
                topic=connection_message.topic,
                partition=client.partition,
            )

            self.handle_subscriber(
                client,
                topic=connection_message.topic,
                group=connection_message.group,
            )
        elif connection_message.event == "publish":
            self.handle_publisher(client, connection_message.topic)
        else:
            logger.error(
                f"Unsupported event: {connection_message.event}. Closing connection..."
            )

        # client socket disconnect
        self.active_clients.remove_client(client.id)
        client.conn.close()

    def _recv_message(self, sock: socket.socket) -> tuple[bytes, bytes] | None:
        """Decode a message received from a client."""
        try:
            # length header is exactly 4 bytes
            raw_length: bytes = self._recv_exactly(sock, HEADER_SIZE)

            # unpack big-endian unsigned int from the length header
            length: int = struct.unpack(">I", raw_length)[0]

            # Step 2: now read exactly that many bytes
            raw_payload: bytes = self._recv_exactly(sock, length)

        except ClientDisconnected:
            return None

        logger.debug(f"HEADER: {raw_length}")
        logger.debug(f"PAYLOAD: {raw_payload}")
        return (raw_length, raw_payload)

    def _parse_message(self, raw_payload: bytes) -> Message:
        """Parse a raw payload into a Message object."""
        try:
            message_content: dict = json.loads(raw_payload.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON payload: {e}")

        try:
            message = Message(
                event=message_content.get("event", ""),
                topic=message_content.get("topic", ""),
                content=message_content.get("content", ""),
                group=message_content.get("group", ""),
            )
        except ValueError as e:
            raise ValueError(f"Invalid message content: {e}")

        return message

    def _recv_exactly(self, sock, n: int) -> bytes:
        """Returns the exact amout of n bytes from the socket."""
        buf = b""

        # the while loop logic handles the case where the socket is closed mid-message
        while len(buf) < n:
            chunk: bytes = sock.recv(n - len(buf))
            if not chunk:
                raise ClientDisconnected("Socket closed mid-message")
            buf += chunk
        return buf

    def handle_publisher(self, client: Client, topic: str) -> None:
        """Publish messages to the topic"""

        logger.info(f"Handling publisher client: {client.id}")
        while client.alive:
            # message: Message | None = self._recv_message(client.conn)
            data: tuple[bytes, bytes] | None = self._recv_message(client.conn)
            if data is None:
                client.alive = False
                continue

            raw_header, raw_payload = data
            message: Message = self._parse_message(raw_payload)
            logger.info(f"Received: {message}")

            # append raw message to the WAL
            full_raw_message: bytes = raw_header + raw_payload
            self.storage.insert(topic, client.partition, full_raw_message)

    def handle_subscriber(self, client: Client, topic: str, group: str) -> None:
        """Respond to the client with the offset message from the topic"""

        logger.info(f"Handling subscriber client: {client.id} ({group}@{topic})")
        while client.alive:
            poll_message: tuple[bytes, bytes] | None = self._recv_message(client.conn)
            if poll_message is None:
                client.alive = False
                continue

            logger.info(f"{topic}@{client.id} Poll recieved...")

            # create a new offset if topic doesn't exist
            if topic not in self.consumer_registry.get_topics(group):
                self.consumer_registry.add_topic(group, topic, self.partitions)

            # get client's offset from the consumer group registry
            offset: int = self.consumer_registry.lookup(group, topic, client.partition)

            if offset >= self.storage.get_partition_size(topic, client.partition):
                resp: dict = {
                    "event": "poll_response",
                    "topic": topic,
                    "group": group,
                    "content": "",
                }
                self.send_poll_response(
                    sock=client.conn, status="NO_NEW_MESSAGES", message=resp
                )
                continue

            # retrieve message from the database matching the offset
            offset_message: dict = self.storage.get(topic, client.partition, offset)

            # increment client offset by 1 for the next poll request
            self.consumer_registry.increment(group, topic, client.partition)

            # respond to the client with the offset message
            self.send_poll_response(
                sock=client.conn, status="OK", message=offset_message
            )

    def _build_payload(self, message: str) -> bytes:
        """Create a message with the topic and message. The message length is added to the first 4 bytes of the payload"""
        length = len(message)
        length_header: bytes = length.to_bytes(HEADER_SIZE, byteorder="big")
        return length_header + message.encode(encoding="utf-8")

    def send_poll_response(
        self, sock: socket.socket, status: str, message: dict
    ) -> None:
        """Send a message to the client"""
        payload: str = json.dumps(
            {"status": status, "message": message.get("content", "")}
        )
        logger.info(f"Sending response: {payload}")
        full_response: bytes = self._build_payload(payload)
        sock.sendall(full_response)
