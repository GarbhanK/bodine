import json
import sys
import time

from bodine.client.publisher import Publisher

try:
    pub = Publisher(broker="localhost:9001", topic="topic1")
except ConnectionRefusedError as e:
    print(f"Connection refused: {e}")
    sys.exit(1)

print(f"Producer created: {pub}")

for i in range(3):
    pub.send(
        json.dumps({"event": "publish", "topic": "topic1", "content": f"hello {i}!"})
    )
    time.sleep(0.5)
