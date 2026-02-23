# import time

from bodine.client.producer import Producer

p = Producer(broker="localhost:9000", topic="greetings")
print(f"Producer created: {p}")

p.send("GREETINGS::hello world")

# for i in range(5):
#     p.send(f"hello world {i}")
#     time.sleep(0.5)

p.send("EXIT")
# p.send("SHUTDOWN")
