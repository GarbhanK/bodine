import datetime as dt
import socket
from dataclasses import dataclass, field
from uuid import UUID, uuid4


@dataclass
class Client:
    address: str
    port: int
    topic: str
    conn: socket.socket = field(default_factory=socket.socket)
    id: UUID = field(default_factory=uuid4)


@dataclass
class Message:
    topic: str
    content: str
    timestamp: float = field(default_factory=dt.datetime.now().timestamp)
    sent: bool = field(default=False)
