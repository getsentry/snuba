from __future__ import annotations

import logging
import signal
from concurrent.futures import FIRST_EXCEPTION, wait
from threading import Event
from typing import Optional, Sequence

from confluent_kafka import Consumer, Message, TopicPartition

from snuba.utils.concurrent import execute


logger = logging.getLogger(__name__)


class CommitLogConsumer:
    def __init__(self, bootstrap_servers: str, shutdown_requested: Event) -> None:
        self.__bootstrap_servers = bootstrap_servers
        self.__shutdown_requested = shutdown_requested

    def __run(self) -> None:
        logger.debug("Starting %r...", self)
        self.__shutdown_requested.wait()

    def run(self) -> Future[None]:
        return execute(self.__run, name="commit-log-consumer")


class SubscriptionConsumer:
    def __init__(self, bootstrap_servers: str, shutdown_requested: Event) -> None:
        self.__bootstrap_servers = bootstrap_servers
        self.__shutdown_requested = shutdown_requested

    def __run(self) -> None:
        logger.debug("Starting %r...", self)
        self.__shutdown_requested.wait()

    def run(self) -> Future[None]:
        return execute(self.__run, name="subscriptions-consumer")


class DatasetConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        shutdown_requested: Event,
    ) -> None:
        self.__bootstrap_servers = bootstrap_servers
        self.__topic = topic
        self.__consumer_group = consumer_group
        self.__shutdown_requested = shutdown_requested

    def __run(self) -> None:
        logger.debug("Starting %r...", self)

        consumer = Consumer(
            {
                "bootstrap.servers": self.__bootstrap_servers,
                "group.id": self.__consumer_group,
            }
        )

        def on_assign(consumer: Consumer, partitions: Sequence[TopicPartition]) -> None:
            logger.debug("Received updated assignment: %r", partitions)

        consumer.subscribe([self.__topic], on_assign=on_assign)

        while not self.__shutdown_requested.is_set():
            message: Optional[Message] = consumer.poll(0.1)
            if message is None:
                continue

    def run(self) -> Future[None]:
        return execute(self.__run, name="dataset-consumer")


def run(
    bootstrap_servers: str = "localhost:9092",
    consumer_group: str = "snuba-subscriptions",
    topic: str = "events",
):
    shutdown_requested = Event()

    # XXX: This will not type check -- these need a common type.
    futures = {
        consumer.run(): consumer
        for consumer in [
            CommitLogConsumer(bootstrap_servers, shutdown_requested),
            SubscriptionConsumer(bootstrap_servers, shutdown_requested),
            DatasetConsumer(
                bootstrap_servers, topic, consumer_group, shutdown_requested
            ),
        ]
    }

    def handler(signal, frame):
        logger.debug("Caught signal %r, requesting shutdown...", signal)
        shutdown_requested.set()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    done, running = wait(futures.keys(), return_when=FIRST_EXCEPTION)

    if not shutdown_requested.is_set():
        logger.warning("Requesting early shutdown due to %r...", done)
        shutdown_requested.set()

    for future, consumer in futures.items():
        try:
            result = future.result()
            logger.debug("%r completed successfully, returning: %s", consumer, result)
        except Exception as error:
            logger.exception("%r completed with error, raising: %s", consumer, error)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(thread)s %(message)s",
    )

    run()
