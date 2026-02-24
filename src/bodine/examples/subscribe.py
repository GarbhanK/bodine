import sys

from bodine.client.subscriber import Subscriber

try:
    s = Subscriber(broker="localhost:9001", topic="greetings")
except ConnectionRefusedError as e:
    print(f"Connection refused: {e}")
    sys.exit(1)

for message in s.poll(default_interval=3.0):
    print(f"received: {message}")
