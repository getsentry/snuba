from __future__ import annotations

import logging
import time
from collections import deque
from concurrent.futures import Future
from datetime import datetime
from typing import Deque, Mapping, MutableMapping, NamedTuple, Optional, Tuple, cast

from arroyo import Message, Topic
from arroyo.backends.abstract import Producer
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies import MessageRejected, ProcessingStrategy
from arroyo.types import BrokerValue, Commit

from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import SubscriptionScheduler
from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.metrics import MetricsBackend

logger = logging.getLogger(__name__)


class CommittableTick(NamedTuple):
    tick: Tick
    # Offset that we can safely committed once the tick is processed.
    # Not necessarily the same as the tick's offset.
    offset_to_commit: Optional[int]


class ProvideCommitStrategy(ProcessingStrategy[Tick]):
    """
    Given a tick message, this step provides the offset commit strategy
    for that tick before submitting the message to the next step.

    We must commit only offsets based on the slowest partition. This
    guarantees that all subscriptions are scheduled at least once and we do
    not miss any even if the scheduler restarts and loses its state.

    Each time a tick message is received by this strategy we need to figure out
    if we can advance the offset that is committed. The offset that is considered
    safe to commit is the latest offset that has been reached or exceeded on every
    partition. It's marked `offset_to_commit` and submitted with the tick to the
    next step.
    """

    # If we receive the following messages:

    #     Tick message:                           A         B         C         D
    #     Partition (in main topic)               0         1         1         0
    #     Message offset (commit log topic):      0         1         2         3

    # - At "A" we don't commit an offset as we don't have any offset for partition 1 yet
    #   (in this scenario there are only 2 partitions)
    # - At "B" we can commit offset 0 as that is the latest offset that has been reached on
    #   both of our 2 partitions
    # - At "C" we can't commit any offset since partition 1 still hasn't advanced from 0
    # - At "D" we can commit offset 2

    def __init__(
        self,
        partitions: int,
        next_step: ProcessingStrategy[CommittableTick],
        metrics: MetricsBackend,
    ) -> None:
        self.__partitions = partitions
        self.__next_step = next_step
        self.__metrics = metrics

        # Store the last message we received for each partition so know when
        # to commit offsets.
        self.__latest_messages_by_partition: MutableMapping[
            int, Optional[BrokerValue[Tick]]
        ] = {index: None for index in range(self.__partitions)}
        self.__offset_low_watermark: Optional[int] = None
        self.__offset_high_watermark: Optional[int] = None

        self.__closed = False

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[Tick]) -> None:
        assert not self.__closed

        assert isinstance(message.value, BrokerValue)

        # Update self.__offset_high_watermark
        self.__update_offset_high_watermark(message.value)

        should_commit = self.__should_commit(message)
        offset_to_commit = self.__offset_high_watermark if should_commit else None

        self.__next_step.submit(
            message.replace(CommittableTick(message.payload, offset_to_commit))
        )
        if should_commit:
            self.__offset_low_watermark = self.__offset_high_watermark

    def __should_commit(self, message: Message[Tick]) -> bool:
        if self.__offset_high_watermark is None:
            return False

        if self.__offset_low_watermark is None:
            return True

        return self.__offset_high_watermark > self.__offset_low_watermark

    def __update_offset_high_watermark(self, value: BrokerValue[Tick]) -> None:
        assert value.partition.index == 0, "Commit log cannot be partitioned"
        tick_partition = value.payload.partition
        assert tick_partition is not None
        self.__latest_messages_by_partition[tick_partition] = value

        # Keep track of the slowest/fastest partition based on the tick's lower timestamp.
        # This is only used for recording the partition lag metric.
        slowest = value.payload.timestamps.lower
        fastest = value.payload.timestamps.lower

        # Keep track of the earliest partition based on its offset in the commit log topic.
        # This is used to determine the offset that we can safely commit.
        # The offset to be committed is the `next_offset`: 1 higher than the offset of
        # the current message
        earliest = value.next_offset

        for partition_message in self.__latest_messages_by_partition.values():
            if partition_message is None:
                return

            partition_timestamp = partition_message.payload.timestamps.lower

            if partition_timestamp < slowest:
                slowest = partition_timestamp

            if partition_timestamp > fastest:
                fastest = partition_timestamp

            if partition_message.offset < earliest:
                earliest = partition_message.offset

        # Record the lag between the fastest and slowest partition
        self.__metrics.timing(
            "partition_lag_ms",
            (fastest - slowest).total_seconds() * 1000,
        )

        if (
            self.__offset_high_watermark is None
            or earliest > self.__offset_high_watermark
        ):
            self.__offset_high_watermark = earliest

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

    If the scheduler mode is PARTITION then there is no buffering and a
    message is always immediately submitted to the next processing step.

    If the scheduler mode is GLOBAL then messages are
    buffered until all partitions are at least up to the timestamp of the
    tick.

    `max_ticks_buffered_per_partition` applies if the scheduler mode is
    GLOBAL. Once the maximum ticks is received for that
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

        self.__closed = False
        self.__record_frequency_seconds = 60
        self.__last_recorded_time: float = 0

    def poll(self) -> None:
        self.__next_step.poll()

    def __record_tick_buffer_length(self) -> None:
        now = time.time()
        if now - self.__last_recorded_time > self.__record_frequency_seconds:
            for partition_index in self.__buffers:
                self.__metrics.gauge(
                    "tick_buffer.queue_size",
                    len(self.__buffers[partition_index]),
                    tags={"partition": str(partition_index)},
                )
            self.__last_recorded_time = now

    def submit(self, message: Message[Tick]) -> None:
        assert not self.__closed

        # If the scheduler mode is immediate or there is only one partition
        # or max_ticks_buffered_per_partition is set to 0,
        # immediately submit message to the next step.
        if (
            self.__mode == SchedulingWatermarkMode.PARTITION
            or self.__partitions == 1
            or self.__max_ticks_buffered_per_partition == 0
        ):
            self.__next_step.submit(message)
            return

        tick_partition = message.payload.partition
        assert tick_partition is not None
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

        # Periodically record the legnth of each buffer
        self.__record_tick_buffer_length()

        while all(len(buffer) > 0 for buffer in self.__buffers.values()):
            earliest_ts = message.payload.timestamps.upper
            assert message.payload.partition is not None
            earliest_ts_partitions = {message.payload.partition}

            for partition_index in self.__buffers:
                buffer = self.__buffers[partition_index]

                tick = buffer[0].payload

                partition_ts = tick.timestamps.upper

                if partition_ts < earliest_ts:
                    earliest_ts = tick.timestamps.upper
                    assert tick.partition is not None
                    earliest_ts_partitions = {tick.partition}

                elif partition_ts == earliest_ts:
                    assert tick.partition is not None
                    earliest_ts_partitions.add(tick.partition)

            for partition_index in earliest_ts_partitions:
                self.__next_step.submit(self.__buffers[partition_index].popleft())

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)


class TickSubscription(NamedTuple):
    tick_message: BrokerValue[CommittableTick]
    subscription_future: Future[BrokerValue[KafkaPayload]]
    offset_to_commit: Optional[int]


class ScheduledSubscriptionQueue:
    """
    Queues ticks with their subscriptions for the ProduceScheduledSubscriptionMessage strategy
    """

    def __init__(self) -> None:
        self.__queues: Deque[
            Tuple[
                BrokerValue[CommittableTick], Deque[Future[BrokerValue[KafkaPayload]]]
            ]
        ] = deque()

    def append(
        self,
        tick_message: BrokerValue[CommittableTick],
        futures: Deque[Future[BrokerValue[KafkaPayload]]],
    ) -> None:
        if len(futures) > 0:
            self.__queues.append((tick_message, futures))

    def peek(self) -> Optional[TickSubscription]:
        if self.__queues:
            tick, futures = self.__queues[0]

            is_last = len(futures) == 1

            offset_to_commit = tick.payload.offset_to_commit if is_last else None

            return TickSubscription(tick, futures[0], offset_to_commit)
        return None

    def popleft(self) -> TickSubscription:
        if self.__queues:
            tick_message, futures = self.__queues[0]
            subscription_future = futures.popleft()

            is_empty = len(futures) == 0

            if is_empty:
                self.__queues.popleft()

            offset_to_commit = (
                tick_message.payload.offset_to_commit if is_empty else None
            )

            return TickSubscription(tick_message, subscription_future, offset_to_commit)

        raise IndexError()

    def __len__(self) -> int:
        return sum([len(futures) for (_tick, futures) in self.__queues])


class ProduceScheduledSubscriptionMessage(ProcessingStrategy[CommittableTick]):
    """ "
    This strategy is responsible for producing a message for all of the subscriptions
    scheduled for a given tick.

    The subscriptions to be scheduled are those that:
    - Have the same partition index as the tick received, and
    - Are scheduled to be run within the time interval defined by the tick
    (taking the jitter into account)

    The subscriptions to be scheduled are encoded and produced to the
    scheduled topic to be picked up by the subscription executor which
    will run the actual query later.

    When a tick is submitted to this strategy, we produce all of it's
    subscriptions and place it in a queue without waiting to see if they
    were succesfully produced or not.

    On each call to poll(), we remove completed futures from the queue.
    If all scheduled subscription messages for that tick were succesfully
    produced and the tick indicates it should be committed, we commit offsets.
    """

    def __init__(
        self,
        schedulers: Mapping[int, SubscriptionScheduler],
        producer: Producer[KafkaPayload],
        scheduled_topic_spec: KafkaTopicSpec,
        commit: Commit,
        stale_threshold_seconds: Optional[int],
        metrics: MetricsBackend,
        slice_id: Optional[int] = None,
    ) -> None:
        self.__schedulers = schedulers
        self.__encoder = SubscriptionScheduledTaskEncoder()
        self.__producer = producer
        self.__scheduled_topic = Topic(
            scheduled_topic_spec.get_physical_topic_name(slice_id)
        )
        self.__commit = commit
        self.__stale_threshold_seconds = stale_threshold_seconds
        self.__metrics = metrics
        self.__closed = False

        # Stores each tick with it's futures
        self.__queue = ScheduledSubscriptionQueue()

        # Not a hard max
        self.__max_buffer_size = 80000

    def poll(self) -> None:
        # Remove completed tasks from the queue and raise if an exception occurred.
        # This method does not attempt to recover from any exception.
        # Also commits any offsets required.
        while self.__queue:
            tick_subscription = self.__queue.peek()

            if tick_subscription is None:
                break

            if not tick_subscription.subscription_future.done():
                break

            self.__queue.popleft()

            if tick_subscription.offset_to_commit:
                offset = {
                    tick_subscription.tick_message.partition: tick_subscription.offset_to_commit
                }
                logger.info("Committing offset: %r", offset)
                self.__commit(offset)

    def submit(self, message: Message[CommittableTick]) -> None:
        assert not self.__closed
        assert isinstance(message.value, BrokerValue)

        # If queue is full, raise MessageRejected to tell the stream
        # processor to pause consuming
        if len(self.__queue) >= self.__max_buffer_size:
            raise MessageRejected

        # Otherwise, add the tick message and all of it's subscriptions to
        # the queue
        tick = message.payload.tick
        assert tick.partition is not None

        if (
            self.__stale_threshold_seconds is not None
            and time.time() - datetime.timestamp(tick.timestamps.lower)
            > self.__stale_threshold_seconds
        ):
            encoded_tasks = []
        else:
            tasks = [task for task in self.__schedulers[tick.partition].find(tick)]

            encoded_tasks = [self.__encoder.encode(task) for task in tasks]

        # Record the amount of time between the message timestamp and when scheduling
        # for that timestamp occurs
        self.__metrics.timing(
            "scheduling_latency",
            (time.time() - datetime.timestamp(message.value.timestamp)) * 1000,
        )

        # If there are no subscriptions for a tick, immediately commit if an offset
        # to commit is provided.
        if len(encoded_tasks) == 0 and message.payload.offset_to_commit is not None:
            offset = {message.value.partition: message.value.payload.offset_to_commit}
            logger.info("Committing offset - no subscriptions: %r", offset)
            self.__commit(offset)
            return

        self.__queue.append(
            message.value,
            deque(
                [
                    self.__producer.produce(self.__scheduled_topic, task)
                    for task in encoded_tasks
                ]
            ),
        )

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        start = time.time()

        while self.__queue:
            remaining = timeout - (time.time() - start) if timeout is not None else None

            if remaining is not None and remaining <= 0:
                logger.warning(f"Timed out with {len(self.__queue)} futures in queue")
                break

            tick_subscription = self.__queue.popleft()

            tick_subscription.subscription_future.result(remaining)

            if tick_subscription.offset_to_commit is not None:
                offset = {
                    tick_subscription.tick_message.partition: tick_subscription.offset_to_commit
                }
                logger.info("Committing offset: %r", offset)
                self.__commit(offset, force=True)
