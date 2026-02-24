class ClientBase:
    def __init__(self):
        pass

    def _build_payload(self, message: str) -> bytes:
        """Create a message with the topic and message. The message length is added to the first 4 bytes of the payload"""
        length = len(message)
        length_header: bytes = length.to_bytes(4, byteorder="big")
        return length_header + message.encode(encoding="utf-8")
