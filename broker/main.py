import socket

PORT: int = 9000
MAX_CONNECTIONS: int = 5

def main() -> None:
    print("Starting broker...")

    broker_active: bool = True

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as e:
        print(f"Socket creation failed with error {e}")

    # bind to local network and specified port
    s.bind(('', PORT))
    print(f"socket binded to {PORT}")

    # put socket into listening mode
    s.listen(MAX_CONNECTIONS)
    print("Socket is listening...")

    while broker_active:
        # establish connnection with client
        c, addr = s.accept() 
        print(f"Got connection from {addr}")

        c.send("Thank you for connecting\n".encode())
        c.close()

        broker_active = False

    print("Shutting down...")

if __name__ == "__main__":
    main()

