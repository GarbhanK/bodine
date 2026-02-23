from client.consumer import Consumer

c = Consumer(broker="broker:9000", topic="greetings")

for message in c.poll():
    print(f"received: {message}")

