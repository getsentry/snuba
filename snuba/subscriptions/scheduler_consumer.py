from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Callable, Mapping, MutableMapping, NamedTuple, Optional, Sequence

from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Consumer
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategyFactory
from arroyo.synchronized import commit_codec
from arroyo.types import Position

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.utils.metrics import MetricsBackend
from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration
from snuba.utils.types import Interval, InvalidRangeError

logger = logging.getLogger(__name__)


class MessageDetails(NamedTuple):
    offset: int
    orig_message_ts: datetime


class Tick(NamedTuple):
    partition: int
    offsets: Interval[int]
    timestamps: Interval[datetime]

    def time_shift(self, delta: timedelta) -> Tick:
        """
        Returns a new ``Tick`` instance that has had the bounds of its time
        interval shifted by the provided delta.
        """
        return Tick(
            self.partition,
            self.offsets,
            Interval(self.timestamps.lower + delta, self.timestamps.upper + delta),
        )


class CommitLogTickConsumer(Consumer[Tick]):
    """
    A modified version of the `TickConsumer` that returns `ticks` constructed
    from messages of a single commit log consumer rather than a synchronized consumer.

    Like the `TickConsumer` the consumer returns a `Tick` message, which is derived
    from the timestamps of the previous two messages associated with a partition.
    The `Tick` now includes the partition number as the partition for the tick
    is no longer the same as the partition of the message.

    Since there is no synchronized consumer involved in this implementation,
    this tick consumer decodes the commit log message in order to get the partition
    number and timestamp corresponding to the original topic being followed by
    the commit log.

    The intervals being returned by this consumer should be equivalent to
    those in the original TickConsumer/Synchronized consumer implementation.
    """

    def __init__(
        self, consumer: Consumer[KafkaPayload], time_shift: Optional[timedelta] = None
    ) -> None:
        self.__consumer = consumer
        self.__previous_messages: MutableMapping[Partition, MessageDetails] = {}
        self.__time_shift = time_shift if time_shift is not None else timedelta()

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        self.__consumer.subscribe(topics, on_assign=on_assign, on_revoke=on_revoke)

    def unsubscribe(self) -> None:
        self.__consumer.unsubscribe()

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[Tick]]:
        message = self.__consumer.poll(timeout)
        if message is None:
            return None

        commit = commit_codec.decode(message.payload)
        assert commit.orig_message_ts is not None

        previous_message = self.__previous_messages.get(commit.partition)

        result: Optional[Message[Tick]]
        if previous_message is not None:
            try:
                time_interval = Interval(
                    previous_message.orig_message_ts, commit.orig_message_ts
                )
            except InvalidRangeError:
                logger.warning(
                    "Could not construct valid time interval between %r and %r!",
                    previous_message,
                    MessageDetails(commit.offset, commit.orig_message_ts),
                    exc_info=True,
                )
                return None
            else:
                result = Message(
                    message.partition,
                    message.offset,
                    Tick(
                        commit.partition.index,
                        Interval(previous_message.offset, commit.offset),
                        time_interval,
                    ).time_shift(self.__time_shift),
                    message.timestamp,
                )
        else:
            result = None

        self.__previous_messages[commit.partition] = MessageDetails(
            commit.offset, commit.orig_message_ts
        )

        return result

    def pause(self, partitions: Sequence[Partition]) -> None:
        self.__consumer.pause(partitions)

    def resume(self, partitions: Sequence[Partition]) -> None:
        self.__consumer.resume(partitions)

    def paused(self) -> Sequence[Partition]:
        return self.__consumer.paused()

    def tell(self) -> Mapping[Partition, int]:
        return self.__consumer.tell()

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        self.__consumer.seek(offsets)

    def stage_positions(self, positions: Mapping[Partition, Position]) -> None:
        return self.__consumer.stage_positions(positions)

    def commit_positions(self) -> Mapping[Partition, Position]:
        return self.__consumer.commit_positions()

    def close(self, timeout: Optional[float] = None) -> None:
        return self.__consumer.close(timeout)

    @property
    def closed(self) -> bool:
        return self.__consumer.closed


class SchedulerBuilder:
    def __init__(
        self,
        entity_name: str,
        partitions: Optional[int],
        consumer_group: str,
        auto_offset_reset: str,
        delay_seconds: Optional[int],
        metrics: MetricsBackend,
    ) -> None:
        self.__entity = get_entity(EntityKey(entity_name))

        storage = self.__entity.get_writable_storage()

        assert (
            storage is not None
        ), f"Entity {entity_name} does not have a writable storage by default."

        stream_loader = storage.get_table_writer().get_stream_loader()

        commit_log_topic_spec = stream_loader.get_commit_log_topic_spec()
        assert commit_log_topic_spec is not None
        self.__commit_log_topic_spec = commit_log_topic_spec

        self.__partitions = (
            partitions
            if partitions is not None
            else stream_loader.get_default_topic_spec().partitions_number
        )

        self.__consumer_group = consumer_group
        self.__auto_offset_reset = auto_offset_reset
        self.__delay_seconds = delay_seconds
        self.__metrics = metrics

    def build_consumer(self) -> StreamProcessor[Tick]:
        return StreamProcessor(
            self.__build_tick_consumer(),
            Topic(self.__commit_log_topic_spec.topic_name),
            self.__build_strategy_factory(),
        )

    def __build_strategy_factory(self) -> ProcessingStrategyFactory[Tick]:
        return SubscriptionSchedulerProcessingFactory(self.__partitions, self.__metrics)

    def __build_tick_consumer(self) -> CommitLogTickConsumer:
        return CommitLogTickConsumer(
            KafkaConsumer(
                build_kafka_consumer_configuration(
                    self.__commit_log_topic_spec.topic,
                    self.__consumer_group,
                    auto_offset_reset=self.__auto_offset_reset,
                ),
            ),
            time_shift=(
                timedelta(seconds=self.__delay_seconds * -1)
                if self.__delay_seconds is not None
                else None
            ),
        )


class MeasurePartitionLag(ProcessingStrategy[Tick]):
    def __init__(
        self,
        partitions: int,
        metrics: MetricsBackend,
        commit: Callable[[Mapping[Partition, Position]], None],
    ) -> None:
        self.__metrics = metrics
        self.__partitions = partitions
        self.__partition_timestamps: MutableMapping[int, Optional[datetime]] = {
            index: None for index in range(partitions)
        }
        self.__commit = commit

    def poll(self) -> None:
        pass

    def submit(self, message: Message[Tick]) -> None:
        if self.__partitions != 1:
            partition_index = message.payload.partition

            partition_timestamp = message.payload.timestamps.upper
            self.__partition_timestamps[partition_index] = partition_timestamp

            earliest = partition_timestamp
            latest = partition_timestamp

            for ts in self.__partition_timestamps.values():
                if ts is None:
                    return

                if earliest is None or ts < earliest:
                    earliest = ts
                if latest is None or ts > latest:
                    latest = ts

            self.__metrics.timing(
                "partition_lag_ms", (latest - earliest).total_seconds() * 1000
            )

        self.__commit({message.partition: Position(message.offset, message.timestamp)})

    def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass

    def join(self, timeout: Optional[float] = None) -> None:
        pass


class SubscriptionSchedulerProcessingFactory(ProcessingStrategyFactory[Tick]):
    def __init__(self, partitions: int, metrics: MetricsBackend) -> None:
        self.__partitions = partitions
        self.__metrics = metrics

    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[Tick]:
        return MeasurePartitionLag(self.__partitions, self.__metrics, commit)
