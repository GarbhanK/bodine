import json
import socket
import struct
import threading
from dataclasses import dataclass
from typing import Any, Callable

from bodine.broker import logs
from bodine.broker.models import PIGDB, Client, ConnRegistry, Message
from bodine.broker.utils import ClientDisconnected

logs.setup_logging()
logger = logs.get_system_logger()
publog = logs.get_publisher_logger()
sublog = logs.get_subscriber_logger()


@dataclass(frozen=True)
class BrokerConfig:
    host: str
    port: int
    max_connections: int


class Broker:
    host: str
    port: int
    max_connections: int
    sock: socket.socket
    active_clients: ConnRegistry
    pigdb: PIGDB

    def __init__(self, cfg: BrokerConfig):
        self.host = cfg.host
        self.port = cfg.port
        self.max_connections = cfg.max_connections
        self.active_clients = ConnRegistry()
        self.pigdb = PIGDB()

        logger.info("Test INFO")
        logger.error("Test ERROR")
        publog.info("Test INFO")
        sublog.info("Test ERROR")

    def __repr__(self) -> str:
        return f"Broker@{self.host}:{self.port}"

    def setup(self) -> None:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as e:
            raise RuntimeError(f"Socket creation failed with error {e}")

        # bind to local network and specified port
        self.sock.bind((self.host, self.port))
        print(f"socket binded to {self.port}")

        # put socket into listening mode
        self.sock.listen(self.max_connections)
        print("Socket is listening...")

    def accept_connections(self) -> None:
        while True:
            try:
                conn, addr = self.sock.accept()
            except ConnectionAbortedError:
                continue  # ignore and keep listening
            except OSError as e:
                print(f"Accept failed: {e}")
                continue

            print(f"Got connection from {addr}")
            self._spawn_worker_thread(self.handle_connection, args=(conn,))

    def handle_connection(self, conn: socket.socket) -> None:
        """Listen for incoming connections on the specified port."""

        # add it to the client connection
        client: Client = Client(conn=conn)
        self.active_clients.add_client(client)

        # handle client connection
        first_message: Message | None = self._recv_message(sock=client.conn)
        print(f"Connection message: {first_message}")

        if first_message is None:
            raise ValueError("Invalid connection message")

        match first_message.action:
            case "subscribe":
                self.handle_subscriber(client, topic=first_message.topic)
            case "publish":
                self.handle_publisher(client, topic=first_message.topic)
            case _:
                raise ValueError(f"Unsupported action: {first_message.action}")

        # client socket disconnect
        self.active_clients.remove_client(client.id)
        self.sock.close()

    # TODO: look into tracking threads as part of the broker class?
    def _spawn_worker_thread(self, func: Callable, args: tuple[Any, ...]):
        thread = threading.Thread(target=func, args=args)
        thread.start()

    def _recv_message(self, sock: socket.socket) -> Message | None:
        """Decode a message received from a client."""
        try:
            # length header is exactly 4 bytes
            raw_length: bytes = self._recv_exactly(sock, 4)

            # unpack big-endian unsigned int from the length header
            length: int = struct.unpack(">I", raw_length)[0]

            # Step 2: now read exactly that many bytes
            raw_payload: bytes = self._recv_exactly(sock, length)
        except ClientDisconnected:
            return None

        try:
            message_content: dict = json.loads(raw_payload.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON payload: {e}")

        try:
            topic: str = message_content["topic"].strip()
            content: str = message_content["message"].strip()
            action: str = message_content["action"].strip()
        except KeyError as e:
            raise ValueError(f"Invalid message format: {message_content}") from e

        return Message(topic=topic.upper(), payload=content, action=action)

    def _recv_exactly(self, sock, n: int) -> bytes:
        """Returns the exact amout of n bytes from the socket."""
        buf = b""

        # the while loop logic handles the case where the socket is closed mid-message
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ClientDisconnected("Socket closed mid-message")
            buf += chunk
        return buf

    def handle_publisher(self, client: Client, topic: str) -> None:
        """Publish a message to the topic"""

        print(f"Handling publisher client: {client.id}")
        while client.alive:
            message: Message | None = self._recv_message(client.conn)
            if message is None:
                client.alive = False
                continue

            print(f"Received message: {message}")

            # insert message into the database
            self.pigdb.insert(message)

    def handle_subscriber(self, client: Client, topic: str) -> None:
        """Respond to the client with the offset message from the topic"""

        print(f"Handling subscriber client: {client.id} ({topic})")
        while client.alive:
            message: Message | None = self._recv_message(client.conn)
            if message is None:
                client.alive = False
                continue

            print(f"{topic}@{client.id} Poll recieved...")

            # TODO: offset tracking should be per-subscriber-group, not coupled to the client id
            # get client's offset from the database

            # create a new offset if topic doesn't exist
            if topic not in client.offsets:
                client.offsets[topic] = 0

            topic_offset: int = client.offsets[topic]
            topic_size: int = self.pigdb.get_topic_size(topic)

            # Raise an error if the offset is greater than the topic size
            if topic_offset > topic_size:
                raise ValueError(
                    f"Invalid Offset: offset {topic_offset} is greater than topic size {topic_size}"
                )

            # if subscriber is at the end of the topic, tell them to wait for new messages
            print(f"{topic_offset=}, {topic_size=}")
            if topic_offset == topic_size:
                resp = Message(action="NO_NEW_MESSAGES", topic=topic, payload="")
                self.send_poll_response(
                    sock=client.conn, status=resp.action, message=resp
                )
                continue

            # retrieve message from the database matching the offset
            offset_message: Message = self.pigdb.get(topic, topic_offset)

            # increment client offset by 1
            client.offsets[topic] += 1

            # respond to the client with the offset message
            self.send_poll_response(
                sock=client.conn, status="OK", message=offset_message
            )

    def _build_payload(self, message: str) -> bytes:
        """Create a message with the topic and message. The message length is added to the first 4 bytes of the payload"""
        length = len(message)
        length_header: bytes = length.to_bytes(4, byteorder="big")
        return length_header + message.encode(encoding="utf-8")

    def send_poll_response(
        self, sock: socket.socket, status: str, message: Message
    ) -> None:
        """Send a message to the client"""
        payload: str = json.dumps({"status": status, "message": message.payload})
        print(f"Sending response: {payload}")
        full_response: bytes = self._build_payload(payload)
        sock.sendall(full_response)
