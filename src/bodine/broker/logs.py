import logging

# ANSI escape codes for terminal colours
COLOURS = {
    "DEBUG": "\033[92m",  # Green
    "INFO": "\033[97m",  # Bright white (no real change on most terminals)
    "WARNING": "\033[93m",  # Yellow
    "ERROR": "\033[91m",  # Red
    "CRITICAL": "\033[95m",  # Magenta
}
RESET = "\033[0m"


class ColouredFormatter(logging.Formatter):
    """Custom formatter that colours the log level prefix."""

    def format(self, record: logging.LogRecord) -> str:
        level = record.levelname
        colour = COLOURS.get(level, RESET)
        # Build the prefix, e.g.  "INFO :: " in the appropriate colour
        prefix = f"{colour}{level}{RESET} :: "
        # Format the rest of the message (just the body, no extra metadata for now)
        return f"{prefix}{record.getMessage()}"


def get_logger(name: str = __name__) -> logging.Logger:
    """
    Return a logger with coloured, level-prefixed console output.

    Usage:
        from logger import get_logger
        log = get_logger(__name__)
        log.debug("Starting up...")
        log.info("Server ready")
        log.warning("Disk space low")
        log.error("Connection refused")
        log.critical("System failure!")
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if get_logger is called more than once
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(ColouredFormatter())
        logger.addHandler(handler)

    logger.setLevel(logging.DEBUG)  # Show every level; adjust as needed
    logger.propagate = False  # Don't bubble up to the root logger
    return logger


# Quick demo when you run this file directly
if __name__ == "__main__":
    log = get_logger("demo")
    log.debug("This is a DEBUG message")
    log.info("This is an INFO message")
    log.warning("This is a WARNING message")
    log.error("This is an ERROR message")
    log.critical("This is a CRITICAL message")
