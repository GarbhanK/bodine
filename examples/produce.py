from client.producer import Producer

p = Producer(broker="broker:9000", topic="greetings")
p.send("hello world")

