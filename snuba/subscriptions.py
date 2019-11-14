from __future__ import annotations

import logging
import signal
from concurrent.futures import FIRST_EXCEPTION, wait
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from threading import Event, Lock
from typing import Iterator, MutableMapping, Optional, Sequence

from confluent_kafka import Consumer, Message, OFFSET_INVALID
from confluent_kafka import TopicPartition as ConfluentTopicPartition

from snuba.utils.concurrent import execute

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TopicPartition:
    topic: str
    partition: int


PartitionOffsetsState = Enum("PartitionState", ["UNKNOWN"])


@dataclass
class PartitionOffsets:
    local: Optional[int]
    remote: MutableMapping[str, Optional[int]]

    @property
    def state(self) -> PartitionOffsetsState:
        if self.local is None:
            return PartitionOffsetsState.UNKNOWN

        for offset in self.remote.values():
            if offset is None:
                return PartitionOffsetsState.UNKNOWN
            else:
                raise NotImplementedError  # TODO


class PartitionOffsetsManager:
    def __init__(self, remote_consumer_groups: Sequence[str]) -> None:
        self.__remote_consumer_groups = remote_consumer_groups

        self.__partitions: MutableMapping[TopicPartition, PartitionOffsets] = {}
        self.__lock = Lock()

    @contextmanager
    def get(self, partition: TopicPartition) -> Iterator[PartitionOffsets]:
        # TODO: This API is too permissive to be a good idea long-term.
        with self.__lock:
            offsets = self.__partitions.get(partition)
            if offsets is None:
                offsets = self.__partitions[partition] = PartitionOffsets(
                    None, {group: None for group in self.__remote_consumer_groups}
                )

            yield offsets
            logger.debug("Offsets for %r have been updated to %r.", partition, offsets)

    def delete(self, partition: TopicPartition) -> None:
        with self.__lock:
            del self.__partitions[partition]
            logger.debug("Offsets for %r have been deleted.", partition)


@dataclass
class CommitData:
    group: str
    partition: TopicPartition
    offset: int


class CommitLogConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        partition_offsets_manager: PartitionOffsetsManager,
        shutdown_requested: Event,
    ) -> None:
        self.__bootstrap_servers = bootstrap_servers
        self.__topic = topic
        self.__consumer_group = consumer_group
        self.__partition_offsets_manager = partition_offsets_manager
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

        consumer.subscribe([self.__topic])

        while not self.__shutdown_requested.is_set():
            message: Optional[Message] = consumer.poll(0.1)
            if message is None:
                continue

            raise NotImplementedError  # TODO: Parse the message.

            data = CommitData()

            if data.group not in self.__consumer_group:
                logger.debug("Skipping %r, group is not in consumer group set.", data)
                continue

            with self.__partition_offsets_manager.get(data.partition) as offsets:
                offsets.remote[data.group] = data.offset

    def run(self) -> Future[None]:
        return execute(self.__run, name="commit-log-consumer")


class PartitionSubscriptions:
    pass


class Loading(PartitionSubscriptions):
    pass


class Streaming(PartitionSubscriptions):
    pass


class PartitionSubscriptionsManager:
    def __init__(self) -> None:
        self.__partitions: MutableMapping[TopicPartition, PartitionSubscriptions] = {}
        self.__lock = Lock()

    @contextmanager
    def get(self, partition: TopicPartition) -> Iterator[PartitionSubscriptions]:
        with self.__lock:
            subscriptions = self.__partitions.get(partition)
            if subscriptions is None:
                subscriptions = self.__partitions[partition] = Loading()
                raise NotImplementedError  # start loading


class SubscriptionConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        partition_subscriptions_manager: PartitionSubscriptionsManager,
        shutdown_requested: Event,
    ) -> None:
        self.__bootstrap_servers = bootstrap_servers
        self.__topic = topic
        self.__consumer_group = consumer_group
        self.__partition_subscriptions_manager = partition_subscriptions_manager
        self.__shutdown_requested = shutdown_requested

    def __run(self) -> None:
        logger.debug("Starting %r...", self)
        self.__shutdown_requested.wait()

    def run(self) -> Future[None]:
        return execute(self.__run, name="subscriptions-consumer")


class SubscribedQueryExecutionConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        partition_offsets_manager: PartitionOffsetsManager,
        partition_subscriptions_manager: PartitionSubscriptionsManager,
        shutdown_requested: Event,
    ) -> None:
        self.__bootstrap_servers = bootstrap_servers
        self.__topic = topic
        self.__consumer_group = consumer_group
        self.__partition_offsets_manager = partition_offsets_manager
        self.__partition_subscriptions_manager = partition_subscriptions_manager
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
            logger.debug("Received updated assignment: %r", partitions)

            offsets: MutableMapping[TopicPartition, int] = {}
            for partition in consumer.committed(partitions):
                tp = TopicPartition(partition.topic, partition.partition)
                offsets[tp] = (
                    partition.offset
                    if partition.offset != OFFSET_INVALID
                    else consumer.get_watermark_offsets(partition)[0]
                )  # TODO: This should be configurable

            consumer.assign(
                [
                    ConfluentTopicPartition(tp.topic, tp.partition, offset)
                    for tp, offset in offsets.items()
                ]
            )

            for partition, offset in offsets.items():
                with self.__partition_offsets_manager.get(partition) as offsets:
                    offsets.local = offset

        def on_revoke(
            consumer: Consumer, partitions: Sequence[ConfluentTopicPartition]
        ) -> None:
            logger.debug("Received partition revocation: %r", partitions)
            for partition in partitions:
                tp = TopicPartition(partition.topic, partition.partition)
                self.__partition_offsets_manager.delete(tp)

        logger.debug("Subscribing to %r...", self.__topic)
        consumer.subscribe([self.__topic], on_assign=on_assign, on_revoke=on_revoke)

        while not self.__shutdown_requested.is_set():
            message: Optional[Message] = consumer.poll(0.1)
            if message is None:
                continue

            error = message.error()
            if error is not None:
                raise Exception(error)

            partition = TopicPartition(message.topic(), message.partition())
            with self.__partition_offsets_manager.get(partition) as offsets:
                # TODO: This should assert this doesn't invalidate our offset
                # invariant. If it does, we need to handle it and pause or roll
                # back.
                offsets.local = message.offset()

    def run(self) -> Future[None]:
        return execute(self.__run, name="subscribed-query-execution-consumer")


def run(
    bootstrap_servers: str = "localhost:9092",
    consumer_group: str = "snuba-subscriptions",
    remote_consumer_groups: Sequence[str] = ["snuba-commit-log"],
    topic: str = "events",
    commit_log_topic: str = "snuba-commit-log",
    subscriptions_topic: str = "events-subscdriptions",
):
    shutdown_requested = Event()

    partition_offsets_manager = PartitionOffsetsManager(remote_consumer_groups)
    partition_subscriptions_manager = PartitionSubscriptionsManager()

    # XXX: This will not type check -- these need a common type.
    futures = {
        consumer.run(): consumer
        for consumer in [
            CommitLogConsumer(
                bootstrap_servers,
                commit_log_topic,
                consumer_group,
                partition_offsets_manager,
                shutdown_requested,
            ),
            SubscriptionConsumer(
                bootstrap_servers,
                subscriptions_topic,
                consumer_group,
                partition_subscriptions_manager,
                shutdown_requested,
            ),
            SubscribedQueryExecutionConsumer(
                bootstrap_servers,
                topic,
                consumer_group,
                partition_offsets_manager,
                partition_subscriptions_manager,
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
        format="%(asctime)s %(levelname)-8s %(thread)s %(message)s",
    )

    run(topic=sys.argv[1])
