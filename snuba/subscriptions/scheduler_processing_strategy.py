import logging
from collections import deque
from datetime import datetime
from typing import Deque, Mapping, Optional, cast

from arroyo import Message
from arroyo.processing.strategies import ProcessingStrategy

from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.metrics import MetricsBackend

logger = logging.getLogger(__name__)


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
