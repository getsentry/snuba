from __future__ import annotations

import logging
import signal
from concurrent.futures import FIRST_EXCEPTION, wait
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from threading import Event, Lock
from typing import Callable, Iterator, MutableMapping, Optional, Sequence

from confluent_kafka import Consumer, Message, OFFSET_INVALID
from confluent_kafka import TopicPartition as ConfluentTopicPartition

from snuba.utils.concurrent import execute

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TopicPartition:
    topic: str
    partition: int


PartitionState = Enum("PartitionState", ["UNKNOWN"])


@dataclass
class PartitionOffsets:
    local: Optional[int]
    remote: MutableMapping[str, Optional[int]]

    @property
    def state(self) -> PartitionState:
        if self.local is None:
            return PartitionState.UNKNOWN

        for offset in self.remote.values():
            if offset is None:
                return PartitionState.UNKNOWN
            else:
                raise NotImplementedError  # TODO


class PartitionOffsetsManager:
    def __init__(self, remote_consumer_groups: Sequence[str]) -> None:
        self.__partitions: MutableMapping[TopicPartition, PartitionOffsets] = {}
        self.__remote_consumer_groups = remote_consumer_groups
        self.__callbacks: Sequence[
            Callable[[TopicPartition, PartitionState], None]
        ] = []

        self.__lock = Lock()

    def add_callback(
        self, callback: Callable[[TopicPartition, PartitionState]]
    ) -> None:
        with self.__lock:
            self.__callbacks.append(callback)

            for partition, offsets in self.__partitions.items():
                callback(partition, offsets.state)

    @contextmanager
    def get(self, partition: TopicPartition) -> Iterator[PartitionOffsets]:
        # TODO: This API is too permissive to be a good idea long-term.
        with self.__lock:
            offsets = self.__partitions.get(partition)
            if offsets is None:
                offsets = self.__partitions[partition] = PartitionOffsets(
                    None, {group: None for group in self.__remote_consumer_groups}
                )
                state = None
            else:
                state = offsets.state

            yield offsets
            logger.debug("Offsets for %r have been updated to %r.", partition, offsets)

            if state != offsets.state:
                # TODO: Should this do anything special if the callback raises?
                for callback in self.__callbacks:
                    callback(partition, offsets.state)


@dataclass
class CommitData:
    group: str
    topic: str
    partition: int
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

            with self.__partition_offsets_manager.get(
                TopicPartition(data.topic, data.partition)
            ) as offsets:
                offsets.remote[data.group] = data.offset

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
            }
        )

        def on_state_change(partition: TopicPartition, state: PartitionState) -> None:
            if state is PartitionState.UNKNOWN:
                logger.debug("%r is in state %r, pausing...", partition, state)
                consumer.pause(
                    [ConfluentTopicPartition(partition.topic, partition.partition)]
                )
            else:
                # TODO: Handle additional states as necessary.
                raise ValueError("received unexpected state")

        self.__partition_offsets_manager.add_callback(on_state_change)

        def on_assign(
            consumer: Consumer, partitions: Sequence[ConfluentTopicPartition]
        ) -> None:
            logger.debug("Received updated assignment: %r", partitions)
            for partition in partitions:
                with self.__partition_offsets_manager.get(
                    TopicPartition(partition.topic, partition.partition)
                ) as offsets:
                    if partition.offset == OFFSET_INVALID:
                        offsets.local = None
                    else:
                        assert partition.offset >= 0
                        offsets.local = partition.offset

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
    remote_consumer_groups: Sequence[str] = ["snuba-commit-log"],
    topic: str = "events",
    commit_log_topic: str = "snuba-commit-log",
    commit_log_consumer_group: str = "snuba-commit-log",
):
    shutdown_requested = Event()

    partition_offsets_manager = PartitionOffsetsManager(remote_consumer_groups)

    # XXX: This will not type check -- these need a common type.
    futures = {
        consumer.run(): consumer
        for consumer in [
            CommitLogConsumer(
                bootstrap_servers,
                commit_log_topic,
                commit_log_consumer_group,
                partition_offsets_manager,
                shutdown_requested,
            ),
            SubscriptionConsumer(bootstrap_servers, shutdown_requested),
            DatasetConsumer(
                bootstrap_servers,
                topic,
                consumer_group,
                partition_offsets_manager,
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
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(thread)s %(message)s",
    )

    run()
