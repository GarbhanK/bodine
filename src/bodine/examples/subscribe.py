import sys

from bodine.client.subscriber import Subscriber


def main() -> None:
    try:
        sub = Subscriber(broker="localhost:9001", topic="topic1")
    except ConnectionRefusedError as e:
        print(f"Connection refused: {e}")
        sys.exit(1)

    try:
        for message in sub.poll(default_interval=3.0):
            print(f"received: {message}")
    except KeyboardInterrupt:
        print("Exiting...")
        sys.exit(0)


if __name__ == "__main__":
    main()
