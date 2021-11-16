import logging
from collections import deque
from concurrent.futures import as_completed
from datetime import datetime
from typing import Callable, Deque, Mapping, MutableMapping, NamedTuple, Optional, cast

from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Producer
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.types import Position

from snuba.datasets.entities import EntityKey
from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.metrics import MetricsBackend
from snuba.utils.scheduler import Scheduler

logger = logging.getLogger(__name__)


class CommittableTick(NamedTuple):
    tick: Tick
    should_commit: bool


class ProvideCommitStrategy(ProcessingStrategy[Tick]):
    """
    Given a tick message, this step provides the offset commit strategy
    for that tick before submitting the message to the next step.

    We must commit only offsets based on the slowest partition. This
    guarantees that all subscriptions are scheduled at least once and we do
    not miss any even if the scheduler restarts and loses its state.

    A tick can be safely committed only if its lower timestamp has also been
    reached on every other partition. If that condition is not met, the message
    is still submitted to the next step with a `should_commit` value of false
    indicating that it's offset is not to be commited.
    """

    def __init__(
        self, partitions: int, next_step: ProcessingStrategy[CommittableTick],
    ) -> None:
        self.__partitions = partitions
        self.__next_step = next_step

        # Store the last message we received for each partition so know when
        # to commit offsets.
        self.__latest_messages_by_partition: MutableMapping[
            int, Optional[Message[Tick]]
        ] = {index: None for index in range(self.__partitions)}
        self.__offset_low_watermark: Optional[int] = None
        self.__offset_high_watermark: Optional[int] = None

        self.__closed = False

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[Tick]) -> None:
        assert not self.__closed

        # Update self.__offset_high_watermark
        self.__update_offset_high_watermark(message)

        should_commit = self.__should_commit(message)

        self.__next_step.submit(
            Message(
                message.partition,
                message.offset,
                CommittableTick(message.payload, should_commit),
                message.timestamp,
                message.next_offset,
            )
        )
        if should_commit:
            self.__offset_low_watermark = message.offset

    def __should_commit(self, message: Message[Tick]) -> bool:
        return (
            self.__offset_low_watermark is None
            or message.offset > self.__offset_low_watermark
        ) and (
            self.__offset_high_watermark is not None
            and message.offset <= self.__offset_high_watermark
        )

    def __update_offset_high_watermark(self, message: Message[Tick]) -> None:
        assert message.partition.index == 0, "Commit log cannot be partitioned"

        tick_partition = message.payload.partition
        self.__latest_messages_by_partition[tick_partition] = message

        slowest = message
        fastest = message
        for partition_message in self.__latest_messages_by_partition.values():
            if partition_message is None:
                return

            partition_timestamp = partition_message.payload.timestamps.lower

            if partition_timestamp < slowest.payload.timestamps.lower:
                slowest = partition_message

            if partition_timestamp > fastest.payload.timestamps.lower:
                fastest = partition_message

        if (
            self.__offset_high_watermark is None
            or slowest.offset > self.__offset_high_watermark
        ):
            self.__offset_high_watermark = slowest.offset

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)


class TickBuffer(ProcessingStrategy[Tick]):
    """
    The TickBuffer buffers ticks until they are ready to be submitted to
    the next processing step.

    The behavior of the TickBuffer depends on which of the two scheduler
    modes applies.

    If the scheduler mode is IMMEDIATE then there is no buffering and a
    message is always immediately submitted to the next processing step.

    If the scheduler mode is WAIT_FOR_SLOWEST_PARTITION then messages are
    buffered until all partitions are at least up to the timestamp of the
    tick.

    `max_ticks_buffered_per_partition` applies if the scheduler mode is
    WAIT_FOR_SLOWEST_PARTITION. Once the maximum ticks is received for that
    partition, we start to submit ticks for processing even if that timestamp
    is not received for all partitions yet.

    This prevents the buffered tick list growing infinitely and scheduling
    to grind to a halt if one partition starts falling far behind for some reason.
    """

    def __init__(
        self,
        mode: SchedulingWatermarkMode,
        partitions: int,
        max_ticks_buffered_per_partition: Optional[int],
        next_step: ProcessingStrategy[Tick],
        metrics: MetricsBackend,
    ) -> None:
        if mode == SchedulingWatermarkMode.GLOBAL:
            assert max_ticks_buffered_per_partition is not None

        self.__mode = mode
        self.__partitions = partitions
        self.__max_ticks_buffered_per_partition = max_ticks_buffered_per_partition
        self.__next_step = next_step
        self.__metrics = metrics

        self.__buffers: Mapping[int, Deque[Message[Tick]]] = {
            index: deque() for index in range(self.__partitions)
        }

        # Stores the latest timestamp we received for any partition. This is
        # just for recording the partition lag.
        self.__latest_ts: Optional[datetime] = None

        self.__closed = False

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[Tick]) -> None:
        assert not self.__closed

        # If the scheduler mode is immediate or there is only one partition
        # or max_ticks_buffered_per_partition is set to 0,
        # immediately submit message to the next step.
        # We don't keep any latest_ts values as it is not relevant.
        if (
            self.__mode == SchedulingWatermarkMode.PARTITION
            or self.__partitions == 1
            or self.__max_ticks_buffered_per_partition == 0
        ):
            self.__next_step.submit(message)
            return

        # Update the latest_ts for metrics
        if (
            self.__latest_ts is None
            or message.payload.timestamps.upper > self.__latest_ts
        ):
            self.__latest_ts = message.payload.timestamps.upper

        tick_partition = message.payload.partition
        self.__buffers[tick_partition].append(message)

        # If the buffer length exceeds `max_ticks_buffered_per_partition`
        # immediately submit the earliest message in that buffer to the next step.
        if len(self.__buffers[tick_partition]) > cast(
            int, self.__max_ticks_buffered_per_partition
        ):
            logger.warning(
                f"Tick buffer exceeded {self.__max_ticks_buffered_per_partition} for partition {tick_partition}"
            )
            self.__next_step.submit(self.__buffers[tick_partition].popleft())
            return

        # If there are any empty buffers, we can't submit anything yet.
        # Otherwise if all the buffers have ticks then we look for the partition/s
        # with the earliest tick (i.e. the tick with the earliest upper timestamp
        # interval value) and submit it to the next step.
        if len(self.__buffers[tick_partition]) > 1:
            return

        while all(len(buffer) > 0 for buffer in self.__buffers.values()):
            earliest_ts = message.payload.timestamps.upper
            earliest_ts_partitions = {message.payload.partition}

            for partition_index in self.__buffers:
                if partition_index == message.payload.partition:
                    continue

                buffer = self.__buffers[partition_index]
                if len(buffer) == 0:
                    return

                tick = buffer[0].payload

                partition_ts = tick.timestamps.upper

                if partition_ts < earliest_ts:
                    earliest_ts = tick.timestamps.upper
                    earliest_ts_partitions = {tick.partition}

                elif partition_ts == earliest_ts:
                    earliest_ts_partitions.add(tick.partition)

            for partition_index in earliest_ts_partitions:
                self.__next_step.submit(self.__buffers[partition_index].popleft())

            # Record the lag between the fastest and slowest partition if we got to this point
            self.__metrics.timing(
                "partition_lag_ms",
                (self.__latest_ts - earliest_ts).total_seconds() * 1000,
            )

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)


class ScheduleSubscriptions(ProcessingStrategy[CommittableTick]):
    """"
    This strategy is responsible for scheduling all of the subscriptions
    for a given tick.

    The subscriptions to be scheduled are those that:
    - Have the same partition index as the tick received, and
    - Are scheduled to be run within the time interval defined by the tick

    For backward compatibility, this assumes that the number of partitions of
    the subscription storage matches the number of partitions of the main topic
    being subscribed to.

    The subscriptions to be scheduled are are encoded and produced to the
    scheduled topic to be picked up by the subscription executor to run
    the actual query later.

    If all of the scheduled subscription messages for the tick are successfully
    produced, and tick specifies `should_commit` = True, then the offset is committed.
    """

    def __init__(
        self,
        entity_key: EntityKey,
        schedulers: Mapping[int, Scheduler[Subscription]],
        producer: Producer[KafkaPayload],
        scheduled_topic_spec: KafkaTopicSpec,
        commit: Callable[[Mapping[Partition, Position]], None],
    ) -> None:
        self.__schedulers = schedulers
        self.__encoder = SubscriptionScheduledTaskEncoder(entity_key)
        self.__producer = producer
        self.__scheduled_topic = Topic(scheduled_topic_spec.topic_name)
        self.__commit = commit
        self.__closed = False

    def poll(self) -> None:
        pass

    def submit(self, message: Message[CommittableTick]) -> None:
        assert not self.__closed

        tick = message.payload.tick

        tasks = self.__schedulers[tick.partition].find(tick.timestamps)
        futures = [
            self.__producer.produce(self.__scheduled_topic, self.__encoder.encode(task))
            for task in tasks
        ]

        for future in as_completed(futures):
            future.result()

        # If all messages are successfully produced and should_commit = True, commit the offset
        if message.payload.should_commit is True:
            self.__commit(
                {message.partition: Position(message.offset, message.timestamp)}
            )

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        pass
