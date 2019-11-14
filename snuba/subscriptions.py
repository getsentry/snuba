from __future__ import annotations

import logging
import signal
from concurrent.futures import FIRST_EXCEPTION, wait
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Event, Lock
from typing import Callable, Iterator, Mapping, MutableMapping, Optional, Sequence

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
        self.__streams: MutableMapping[Stream, StreamState] = {}
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
                if self.__streams:
                    on_assign(self.__streams)

            if on_revoke is not None:
                self.__revocation_callbacks.append(on_revoke)

    def assign(self, streams: Sequence[Stream, StreamState]) -> None:
        with self.__lock:
            logger.debug("Received stream assignment: %r", streams)

            assert not self.__streams

            self.__streams = streams

            logger.debug("Invoking stream assignment callbacks...")
            for callback in self.__assignment_callbacks:
                callback(self.__streams)

    def revoke(self, streams: Sequence[Stream]) -> None:
        with self.__lock:
            logger.debug("Recieved stream revocation: %r", streams)

            assert set(streams) == self.__streams.keys()

            logger.debug("Invoking stream revocation callbacks...")
            for callback in self.__revocation_callbacks:
                callback(self.__streams)

            self.__streams.clear()

    @contextmanager
    def get(self, stream: Stream) -> Iterator[StreamState]:
        with self.__lock:
            yield self.__streams[stream]


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

        def on_assign(streams: MutableMapping[Stream, StreamState]) -> None:
            # XXX: This is not thread safe -- this happens in the query
            # executor thread, and will need to be pushed back to the
            # commit log consumer thread instead.
            logger.debug("Updating commit log consumer assignment...")

            # TODO: This transformation should be configurable.
            commit_log_streams = set(
                [
                    Stream(f"{stream.topic}-commit-log", stream.partition)
                    for stream in streams.keys()
                ]
            )

            logger.debug("Assigning %r to %r...", commit_log_streams, consumer)
            consumer.assign(
                [
                    ConfluentTopicPartition(stream.topic, stream.partition)
                    for stream in commit_log_streams
                ]
            )

        self.__stream_state_manager.add_callbacks(on_assign=on_assign)

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

        def on_assign(streams: MutableMapping[Stream, StreamState]) -> None:
            # XXX: This is not thread safe -- this happens in the query
            # executor thread, and will need to be pushed back to the
            # subscription consumer thread instead.
            logger.debug("Updating subscription consumer assignment...")

            # TODO: This transformation should be configurable.
            subscription_streams = set(
                [
                    Stream(f"{stream.topic}-subscriptions", stream.partition)
                    for stream in streams.keys()
                ]
            )

            logger.debug("Assigning %r to %r...", subscription_streams, consumer)
            consumer.assign(
                [
                    ConfluentTopicPartition(stream.topic, stream.partition)
                    for stream in subscription_streams
                ]
            )

        self.__stream_state_manager.add_callbacks(on_assign=on_assign)

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
        remote_consumer_groups: Sequence[str],
        stream_state_manager: StreamStateManager,
        shutdown_requested: Event,
    ) -> None:
        self.__bootstrap_servers = bootstrap_servers
        self.__topic = topic
        self.__consumer_group = consumer_group
        self.__remote_consumer_groups = remote_consumer_groups
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
            streams: MutableMapping[Stream, StreamState] = {}

            for partition in consumer.committed(partitions):
                # TODO: The starting offset should be configurable.
                # TODO: This seems like the wrong place to instantiate the
                # remote consumer groups, but not sure where else it'd go...
                streams[Stream(partition.topic, partition.partition)] = StreamState(
                    offsets=Offsets(
                        local=partition.offset
                        if partition.offset != OFFSET_INVALID
                        else consumer.get_watermark_offsets(partition)[0],
                        remote={group: None for group in self.__remote_consumer_groups},
                    ),
                    subscriptions=Subscriptions(),
                )

            consumer.assign(
                [
                    ConfluentTopicPartition(
                        stream.topic, stream.partition, state.offsets.local
                    )
                    for stream, state in streams.items()
                ]
            )

            self.__stream_state_manager.assign(streams)

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

            stream = Stream(message.topic(), message.partition())
            with self.__stream_state_manager.get(stream) as state:
                print(stream, state)
                state.offsets.local = message.offset() + 1

    def run(self) -> Future[None]:
        return execute(self.__run, name="subscribed-query-execution-consumer")


def run(
    bootstrap_servers: str = "localhost:9092",
    consumer_group: str = "snuba-subscriptions",
    topic: str = "events",
    remote_consumer_groups: Sequence[str] = ["snuba-commit-log"],
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
                remote_consumer_groups,
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
