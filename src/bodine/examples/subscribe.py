import sys

from bodine.client.subscriber import Subscriber

try:
    sub = Subscriber(broker="localhost:9001", topic="topic1")
except ConnectionRefusedError as e:
    print(f"Connection refused: {e}")
    sys.exit(1)

for message in sub.poll(default_interval=3.0):
    print(f"received: {message}")
