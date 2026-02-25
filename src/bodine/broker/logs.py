import logging


class RoleFormatter(logging.Formatter):
    COLORS = {
        "PUBLISHER": "\033[94m",  # Blue
        "SUBSCRIBER": "\033[92m",  # Green
        "SYSTEM": "\033[97m",  # White
        "ERROR": "\033[91m",  # Red
    }
    RESET = "\033[0m"

    ROLE_PREFIX = {
        "PUBLISHER": "P",
        "SUBSCRIBER": "S",
        "SYSTEM": "I",
        "ERROR": "E",
    }

    def format(self, record):
        role = getattr(record, "role", "SYSTEM")

        color = self.COLORS.get(role, self.COLORS["SYSTEM"])
        prefix = self.ROLE_PREFIX.get(role, "I")

        record.msg = f"{color}{prefix}{self.RESET} | {record.msg}"
        return super().format(record)


def setup_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(RoleFormatter("%(message)s"))

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers = [handler]

    # switch to this one
    logging.basicConfig(level=logging.INFO, format="%(threadName)s | %(message)s")
