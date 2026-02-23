import json

# import time
from bodine.client.producer import Producer

p = Producer(broker="localhost:9000", topic="greetings")
print(f"Producer created: {p}")

message = json.dumps({"topic": "greetings", "message": "hello, world!"})
print(message)

p.send(message)

# for i in range(5):
#     p.send(f"hello world {i}")
#     time.sleep(0.5)

p.send("EXIT")
# p.send("SHUTDOWN")
