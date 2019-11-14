from __future__ import annotations

import logging
import signal
from concurrent.futures import FIRST_EXCEPTION, wait
from dataclasses import dataclass
from threading import Event, Lock
from typing import Callable, Mapping, MutableMapping, Optional, Sequence

from confluent_kafka import Consumer, Message, OFFSET_INVALID
from confluent_kafka import TopicPartition as ConfluentTopicPartition

from snuba.utils.concurrent import execute

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Stream:
    topic: str
    partition: int


@dataclass
class Offsets:
    local: Optional[int]
    remote: MutableMapping[str, Optional[int]]


class Subscriptions:
    pass


@dataclass
class StreamState:
    offsets: Offsets
    subscriptions: Subscriptions


class StreamStateManager:
    def __init__(self) -> None:
        self.__assignment: MutableMapping[Stream, StreamState] = {}
        self.__assignment_callbacks: Sequence[
            Callable[[Mapping[Stream, StreamState]], None]
        ] = []
        self.__revocation_callbacks: Sequence[
            Callable[[Mapping[Stream, StreamState]], None]
        ] = []
        self.__lock = Lock()

    def add_callbacks(
        self,
        on_assign: Optional[Callable[[Mapping[Stream, StreamState]], None]] = None,
        on_revoke: Optional[Callable[[Mapping[Stream, StreamState]], None]] = None,
    ) -> None:
        with self.__lock:
            if on_assign is not None:
                self.__assignment_callbacks.append(on_assign)
                if self.__assignment:
                    on_assign(self.__assignment)

            if on_revoke is not None:
                self.__revocation_callbacks.append(on_revoke)

    def assign(self, streams: Sequence[Stream]) -> None:
        with self.__lock:
            logger.debug("Received stream assignment: %r", streams)

            for stream in streams:
                assert stream not in self.__assignment_callbacks
                self.__assignment[stream] = StreamState(
                    offsets=Offsets(local=None, remote={group: None for group in []}),
                    subscriptions=Subscriptions(),
                )

            logger.debug("Invoking stream assignment callbacks...")
            for callback in self.__assignment_callbacks:
                callback(self.__assignment)

    def revoke(self, streams: Sequence[Stream]) -> None:
        with self.__lock:
            logger.debug("Recieved stream revocation: %r", streams)

            logger.debug("Invoking stream revocation callbacks...")
            for callback in self.__revocation_callbacks:
                callback(self.__assignment)

            for stream in streams:
                del self.__assignment[stream]


class CommitLogConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        consumer_group: str,
        stream_state_manager: StreamStateManager,
        shutdown_requested: Event,
    ) -> None:
        self.__bootstrap_servers = bootstrap_servers
        self.__consumer_group = consumer_group
        self.__stream_state_manager = stream_state_manager
        self.__shutdown_requested = shutdown_requested

    def __run(self) -> None:
        logger.debug("Starting %r...", self)

        consumer = Consumer(
            {
                "bootstrap.servers": self.__bootstrap_servers,
                "group.id": self.__consumer_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "beginning",
            }
        )

        while not self.__shutdown_requested.is_set():
            message: Optional[Message] = consumer.poll(0.1)
            if message is None:
                continue

            raise NotImplementedError

    def run(self) -> Future[None]:
        return execute(self.__run, name="commit-log-consumer")


class SubscriptionConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        consumer_group: str,
        stream_state_manager: StreamStateManager,
        shutdown_requested: Event,
    ) -> None:
        self.__bootstrap_servers = bootstrap_servers
        self.__consumer_group = consumer_group
        self.__stream_state_manager = stream_state_manager
        self.__shutdown_requested = shutdown_requested

    def __run(self) -> None:
        logger.debug("Starting %r...", self)

        consumer = Consumer(
            {
                "bootstrap.servers": self.__bootstrap_servers,
                "group.id": self.__consumer_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "beginning",
            }
        )

        while not self.__shutdown_requested.is_set():
            message: Optional[Message] = consumer.poll(0.1)
            if message is None:
                continue

            raise NotImplementedError

    def run(self) -> Future[None]:
        return execute(self.__run, name="subscriptions-consumer")


class SubscribedQueryExecutionConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        stream_state_manager: StreamStateManager,
        shutdown_requested: Event,
    ) -> None:
        self.__bootstrap_servers = bootstrap_servers
        self.__topic = topic
        self.__consumer_group = consumer_group
        self.__stream_state_manager = stream_state_manager
        self.__shutdown_requested = shutdown_requested

    def __run(self) -> None:
        logger.debug("Starting %r...", self)

        consumer = Consumer(
            {
                "bootstrap.servers": self.__bootstrap_servers,
                "group.id": self.__consumer_group,
                "auto.offset.reset": "error",
                "enable.auto.commit": False,
            }
        )

        def on_assign(
            consumer: Consumer, partitions: Sequence[ConfluentTopicPartition]
        ) -> None:
            stream_offsets: MutableMapping[Stream, int] = {}

            for partition in consumer.committed(partitions):
                stream_offsets[Stream(partition.topic, partition.partition)] = (
                    partition.offset
                    if partition.offset != OFFSET_INVALID
                    else consumer.get_watermark_offsets(partition)[0]
                )  # TODO: This should be configurable

            consumer.assign(
                [
                    ConfluentTopicPartition(stream.topic, stream.partition, offset)
                    for stream, offset in stream_offsets.items()
                ]
            )

            self.__stream_state_manager.assign([*stream_offsets.keys()])

        def on_revoke(
            consumer: Consumer, partitions: Sequence[ConfluentTopicPartition]
        ) -> None:
            self.__stream_state_manager.revoke(
                [
                    Stream(partition.topic, partition.partition)
                    for partition in partitions
                ]
            )

        logger.debug("Subscribing to %r...", self.__topic)
        consumer.subscribe([self.__topic], on_assign=on_assign, on_revoke=on_revoke)

        while not self.__shutdown_requested.is_set():
            message: Optional[Message] = consumer.poll(0.1)
            if message is None:
                continue

            error = message.error()
            if error is not None:
                raise Exception(error)

    def run(self) -> Future[None]:
        return execute(self.__run, name="subscribed-query-execution-consumer")


def run(
    bootstrap_servers: str = "localhost:9092",
    consumer_group: str = "snuba-subscriptions",
    remote_consumer_groups: Sequence[str] = ["snuba-commit-log"],
    topic: str = "events",
):
    shutdown_requested = Event()

    stream_state_manager = StreamStateManager()

    # XXX: This will not type check -- these need a common type.
    futures = {
        consumer.run(): consumer
        for consumer in [
            CommitLogConsumer(
                bootstrap_servers,
                consumer_group,
                stream_state_manager,
                shutdown_requested,
            ),
            SubscriptionConsumer(
                bootstrap_servers,
                consumer_group,
                stream_state_manager,
                shutdown_requested,
            ),
            SubscribedQueryExecutionConsumer(
                bootstrap_servers,
                topic,
                consumer_group,
                stream_state_manager,
                shutdown_requested,
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
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(threadName)s %(message)s",
    )

    run(topic=sys.argv[1])
