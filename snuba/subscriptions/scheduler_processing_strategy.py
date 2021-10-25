import logging
from collections import deque
from typing import Deque, Mapping, MutableMapping, NamedTuple, Optional, cast

from arroyo import Message
from arroyo.processing.strategies import ProcessingStrategy

from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.metrics import MetricsBackend

logger = logging.getLogger(__name__)


class CommitableTick(NamedTuple):
    tick: Tick
    should_commit: bool


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
        next_step: ProcessingStrategy[CommitableTick],
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

        # Store the last message we received for each partition so know when
        # to commit offsets.
        self.__latest_messages_by_partition: MutableMapping[
            int, Optional[Message[Tick]]
        ] = {index: None for index in range(self.__partitions)}
        self.__last_committed_offset: Optional[int] = None
        self.__max_offset_to_commit: Optional[int] = None

        self.__closed = False

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[Tick]) -> None:
        assert not self.__closed

        # Update self.__max_offset_to_commit
        self.__update_max_offset_to_commit(message)

        # If the scheduler mode is immediate or there is only one partition
        # or max_ticks_buffered_per_partition is set to 0,
        # immediately submit message to the next step.
        # We don't keep any latest_ts values as it is not relevant.
        if (
            self.__mode == SchedulingWatermarkMode.PARTITION
            or self.__partitions == 1
            or self.__max_ticks_buffered_per_partition == 0
        ):

            should_commit = self.__should_commit(message)

            self.__next_step.submit(
                Message(
                    message.partition,
                    message.offset,
                    CommitableTick(message.payload, should_commit),
                    message.timestamp,
                    message.next_offset,
                )
            )
            if should_commit:
                self.__last_committed_offset = message.offset

            return

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

            earliest_message = self.__buffers[tick_partition].popleft()

            should_commit = self.__should_commit(earliest_message)

            self.__next_step.submit(
                Message(
                    earliest_message.partition,
                    earliest_message.offset,
                    CommitableTick(earliest_message.payload, should_commit),
                    earliest_message.timestamp,
                    earliest_message.next_offset,
                )
            )

            if should_commit:
                self.__last_committed_offset = earliest_message.offset

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
                current_message = self.__buffers[partition_index].popleft()
                should_commit = self.__should_commit(current_message)
                self.__next_step.submit(
                    Message(
                        current_message.partition,
                        current_message.offset,
                        CommitableTick(current_message.payload, should_commit),
                        current_message.timestamp,
                        current_message.next_offset,
                    )
                )

                if should_commit:
                    self.__last_committed_offset = current_message.offset

    def __update_max_offset_to_commit(self, message: Message[Tick]) -> None:
        """
        Regardless of the scheduler mode we only commit offsets based on the slowest
        partition. This guarantees that all subscriptions are scheduled at
        least once and we do not miss any even if the scheduler restarts and loses
        its state. If the scheduler restarts an one partition is far behind, it
        can lead to the same subscription being scheduled more than once especially
        if we are in `partition` mode.

        Also records the partition lag.
        """
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
            self.__max_offset_to_commit is None
            or slowest.offset > self.__max_offset_to_commit
        ):
            self.__max_offset_to_commit = slowest.offset

            # Record the lag between the fastest and slowest partition
            self.__metrics.timing(
                "partition_lag_ms",
                (
                    fastest.payload.timestamps.lower - slowest.payload.timestamps.lower
                ).total_seconds()
                * 1000,
            )

    def __should_commit(self, message: Message[Tick]) -> bool:
        return (
            self.__last_committed_offset is None
            or message.offset > self.__last_committed_offset
        ) and (
            self.__max_offset_to_commit is not None
            and message.offset <= self.__max_offset_to_commit
        )

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)
