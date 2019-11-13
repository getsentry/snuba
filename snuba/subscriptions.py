import logging


logger = logging.getLogger(__name__)


def run_commit_log_consumer(bootstrap_servers: str) -> None:
    logger.debug("Starting commit log consumer...")
    raise NotImplementedError


def run_subscription_consumer(bootstrap_servers: str) -> None:
    logger.debug("Starting subscription consumer...")
    raise NotImplementedError


def run_dataset_consumer(bootstrap_servers: str) -> None:
    logger.debug("Starting dataset consumer...")
    raise NotImplementedError


def run(
    bootstrap_servers: str = "localhost:9092",
    consumer_group: str = "snuba-subscriptions",
    topic: str = "events",
):
    run_commit_log_consumer(bootstrap_servers)
    run_subscription_consumer(bootstrap_servers)
    run_dataset_consumer(bootstrap_servers)


if __name__ == "__main__":
    run()
