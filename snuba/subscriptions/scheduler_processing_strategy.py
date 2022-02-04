from __future__ import annotations

import logging
import time
from collections import deque
from concurrent.futures import Future
from datetime import datetime
from typing import (
    Callable,
    Deque,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Tuple,
    cast,
)

from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Producer
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies import MessageRejected, ProcessingStrategy
from arroyo.types import Position

from snuba.datasets.table_storage import KafkaTopicSpec
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import SubscriptionScheduler
from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.metrics import MetricsBackend

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
            int, Optional[Message[Tick]]
        ] = {index: None for index in range(self.__partitions)}
        self.__offset_low_watermark: Optional[int] = None
        self.__offset_high_watermark: Optional[int] = None

        self.__closed = False

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[Tick]) -> None:
        start = time.time()

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

        self.__metrics.timing(
            "ProvideCommitStrategy.submit", (time.time() - start) * 1000
        )

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
        assert tick_partition is not None
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

    If the scheduler mode is PARTITION then there is no buffering and a
    message is always immediately submitted to the next processing step.

    If the scheduler mode is GLOBAL then messages are
    buffered until all partitions are at least up to the timestamp of the
    tick.

    `max_ticks_buffered_per_partition` applies if the scheduler mode is
    PARTITION. Once the maximum ticks is received for that
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
        start = time.time()

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

        # If there are any empty buffers, we can't submit anything yet.
        # Otherwise if all the buffers have ticks then we look for the partition/s
        # with the earliest tick (i.e. the tick with the earliest upper timestamp
        # interval value) and submit it to the next step.
        if len(self.__buffers[tick_partition]) > 1:
            return

        while all(len(buffer) > 0 for buffer in self.__buffers.values()):
            earliest_ts = message.payload.timestamps.upper
            assert message.payload.partition is not None
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
                    assert tick.partition is not None
                    earliest_ts_partitions = {tick.partition}

                elif partition_ts == earliest_ts:
                    assert tick.partition is not None
                    earliest_ts_partitions.add(tick.partition)

            for partition_index in earliest_ts_partitions:
                self.__next_step.submit(self.__buffers[partition_index].popleft())

            # Record the lag between the fastest and slowest partition if we got to this point
            self.__metrics.timing(
                "partition_lag_ms",
                (self.__latest_ts - earliest_ts).total_seconds() * 1000,
            )

            self.__metrics.timing("TickBuffer.submit", (time.time() - start) * 1000)

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)


class TickSubscription(NamedTuple):
    tick_message: Message[CommittableTick]
    subscription_future: Future[Message[KafkaPayload]]
    should_commit: bool


class ScheduledSubscriptionQueue:
    """
    Queues ticks with their subscriptions for the ProduceScheduledSubscriptionMessage strategy
    """

    def __init__(self) -> None:
        self.__queues: Deque[
            Tuple[Message[CommittableTick], Deque[Future[Message[KafkaPayload]]]]
        ] = deque()

    def append(
        self,
        tick_message: Message[CommittableTick],
        futures: Deque[Future[Message[KafkaPayload]]],
    ) -> None:
        if len(futures) > 0:
            self.__queues.append((tick_message, futures))

    def peek(self) -> Optional[TickSubscription]:
        if self.__queues:
            tick, futures = self.__queues[0]
            return TickSubscription(tick, futures[0], len(futures) == 1)
        return None

    def popleft(self) -> TickSubscription:
        if self.__queues:
            tick_message, futures = self.__queues[0]
            subscription_future = futures.popleft()

            is_empty = len(futures) == 0

            if is_empty:
                self.__queues.popleft()

            should_commit = is_empty and tick_message.payload.should_commit == True

            return TickSubscription(tick_message, subscription_future, should_commit)

        raise IndexError()

    def __len__(self) -> int:
        return sum([len(futures) for (_tick, futures) in self.__queues])


class ProduceScheduledSubscriptionMessage(ProcessingStrategy[CommittableTick]):
    """"
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
        commit: Callable[[Mapping[Partition, Position]], None],
        metrics: MetricsBackend,
    ) -> None:
        self.__schedulers = schedulers
        self.__encoder = SubscriptionScheduledTaskEncoder()
        self.__producer = producer
        self.__scheduled_topic = Topic(scheduled_topic_spec.topic_name)
        self.__commit = commit
        self.__metrics = metrics
        self.__closed = False

        # Stores each tick with it's futures
        self.__queue = ScheduledSubscriptionQueue()

        # Not a hard max
        self.__max_buffer_size = 10000

    def poll(self) -> None:
        start = time.time()
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

            if tick_subscription.should_commit:
                self.__commit(
                    {
                        tick_subscription.tick_message.partition: Position(
                            tick_subscription.tick_message.offset,
                            tick_subscription.tick_message.timestamp,
                        )
                    }
                )

        self.__metrics.timing(
            "ProduceScheduledSubscriptionMessage.poll", (time.time() - start) * 1000
        )

    def submit(self, message: Message[CommittableTick]) -> None:
        assert not self.__closed

        # If queue is full, raise MessageRejected to tell the stream
        # processor to pause consuming
        if len(self.__queue) >= self.__max_buffer_size:
            raise MessageRejected

        # Otherwise, add the tick message and all of it's subscriptions to
        # the queue
        tick = message.payload.tick
        assert tick.partition is not None

        start_find_tasks = time.time()
        tasks = [task for task in self.__schedulers[tick.partition].find(tick)]
        self.__metrics.timing(
            "ScheduledSubscriptionTask.find_tasks",
            (time.time() - start_find_tasks) * 1000,
        )

        encoded_tasks = [self.__encoder.encode(task) for task in tasks]

        self.__queue.append(
            message,
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

            if tick_subscription.should_commit:
                self.__commit(
                    {
                        tick_subscription.tick_message.partition: Position(
                            tick_subscription.tick_message.offset,
                            tick_subscription.tick_message.timestamp,
                        )
                    }
                )
