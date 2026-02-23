import json

# import time
from bodine.client.producer import Producer

p = Producer(broker="localhost:9001", topic="greetings")
print(f"Producer created: {p}")

message = json.dumps(
    {"action": "produce", "topic": "greetings", "message": "hello, world!"}
)

print(message)
p.send(message)
# p.send("SHUTDOWN")
