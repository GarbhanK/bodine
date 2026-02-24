from bodine.broker import logs
from bodine.broker.broker import Broker, BrokerConfig

PORT: int = 9001
MAX_CONNECTIONS: int = 5

logs.setup_logging()
logger = logs.get_system_logger()


def main() -> None:
    logger.info("Starting broker...")

    # TODO: Implement a way to configure the broker
    cfg = BrokerConfig(
        host="127.0.0.1",
        port=PORT,
        max_connections=MAX_CONNECTIONS,
    )
    broker = Broker(cfg)
    broker.setup()

    try:
        broker.accept_connections()

    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down...")
        broker.sock.close()


if __name__ == "__main__":
    main()
