import json
import sys
import time

from bodine.client.publisher import Publisher

try:
    p = Publisher(broker="localhost:9001", topic="greetings")
except ConnectionRefusedError as e:
    print(f"Connection refused: {e}")
    sys.exit(1)

print(f"Producer created: {p}")

message = json.dumps(
    {"event": "publish", "topic": "greetings", "content": "hello, world!"}
)

print(message)

for i in range(3):
    p.send(message)
    time.sleep(0.5)
