from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping, Optional, Sequence, Union

from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME, Consumer, TopicPartition


class Task:
    """Represents a single task to be executed."""


@dataclass(frozen=True)
class Position:
    """
    Represents a position in a Kafka partition.

    This represents an offset, as well as a point in time.
    """

    offset: int
    timestamp: float


@dataclass(frozen=True)
class Interval:
    """
    Represents a range between two positions in a Kafka partition.
    """

    lower: Position
    upper: Position

    def __post_init__(self) -> None:
        assert self.upper.offset > self.lower.offset
        assert self.upper.timestamp >= self.lower.timestamp

    def shift(self, position: Position) -> Interval:
        """
        Return a new ``Interval`` instance, using the provided position as
        the new upper bound, and the existing upper bound as the new lower
        bound.
        """
        return Interval(self.upper, position)


@dataclass(frozen=True)
class TaskSet:
    """
    Represents a collection of tasks to be executed.
    """

    partition: int
    interval: Interval
    tasks: Sequence[Task]


class Scheduler:
    """
    Identifies tasks to be executed based on the data within a Kafka topic.
    """

    def __init__(self, configuration: Mapping[str, Any], topic: str) -> None:
        self.__configuration = configuration
        self.__topic = topic

        # There are three valid states for a partition in this mapping:
        # 1. Partitions that have been assigned but have not yet had any messages
        # consumed from them will have a value of ``None``.
        # 2. Partitions that have had a single message consumed will have a
        # value of ``Position``.
        # 3. Partitions that have had more than one message consumed will have
        # a value of ``Interval``.
        #
        # Take this example, where a partition contains three messages (MA, MB,
        # MC) and a scheduled task (T1-TN).
        #
        #    Messages:           MA        MB        MC
        #    Timeline: +---------+---------+---------+---------
        #    Tasks:    ^    ^    ^    ^    ^    ^    ^    ^
        #              T1   T2   T3   T4   T5   T6   T7   T8
        #
        # In this example, when we are assigned the partition, the state is set
        # to ``None``. After consuming Message A ("MA"), the partition state
        # becomes the Position derived from MA. No tasks will have yet been
        # executed.
        #
        # When Message B is consumed, the partition state becomes the
        # ``Interval`` instance derived from ``(MA, MB)``. At this point, T4
        # and T5 (the tasks that are scheduled between the timestamps of
        # messages "MA" and "MB") will be included in the ``TaskSet`` returned
        # by the ``poll`` call. T3 will not be included, since it was
        # presumably contained within a ``TaskSet`` instance returned by a
        # previous ``poll`` call. The lower bound ("MA" in this case) is
        # exclusive, while the upper bound ("MB") is inclusive.
        #
        # When all tasks in the ``TaskSet`` have been successfully evaluated,
        # committing the task set will commit the *lower bound* offset of this
        # task set. The lower bound is selected so that on consumer restart or
        # rebalance, the message that has an offset greater than the lower
        # bound (in our case, "MB") will be the first message consumed. The
        # next tasks to be executed will be those that are scheduled between
        # the timestamps of "MB" and "MC" (again: lower bound exclusive, upper
        # bound inclusive): T6 and T7.
        self.__partitions: MutableMapping[int, Union[None, Position, Interval]] = {}

        self.__consumer = Consumer(configuration)
        self.__consumer.subscribe(
            [topic], on_assign=self.__on_assign, on_revoke=self.__on_revoke
        )

    def __on_assign(
        self, consumer: Consumer, assignment: Sequence[TopicPartition]
    ) -> None:
        for tp in assignment:
            if tp.partition not in self.__partitions:
                self.__partitions[tp.partition] = None

    def __on_revoke(
        self, consumer: Consumer, assignment: Sequence[TopicPartition]
    ) -> None:
        for tp in assignment:
            del self.__partitions[tp.partition]

    def poll(self, timeout: Optional[float] = None) -> Optional[TaskSet]:
        """
        Poll to see if any tasks are ready to execute.
        """
        message = self.__consumer.poll(timeout)
        if message is None:
            return None

        error = message.error()
        if error is not None:
            raise error

        timestamp_type, timestamp = message.timestamp()
        assert timestamp_type == TIMESTAMP_LOG_APPEND_TIME

        position = Position(message.offset(), timestamp / 1000.0)

        partition = message.partition()
        state = self.__partitions[partition]

        if state is None:
            state = position
            result = None
        elif isinstance(state, Position):
            state = Interval(state, position)
            result = None
        elif isinstance(state, Interval):
            state = state.shift(position)
            # TODO: Get tasks that are scheduled between the interval.
            result = TaskSet(partition, state, [])
        else:
            raise ValueError("unexpected state")

        self.__partitions[partition] = state

        return result

    def done(self, tasks: TaskSet) -> None:
        """
        Mark all tasks within the task set as completed.
        """
        assert self.__partitions[tasks.partition] == tasks.interval

        # N.B.: ``store_offset``'s handling of offset values differs from that
        # of ``commit``! The offset passed to ``store_offset`` will be the
        # offset of the first message read when the consumer restarts.
        self.__consumer.store_offsets(
            offsets=[
                TopicPartition(self.__topic, tasks.partition, tasks.interval.upper)
            ]
        )
