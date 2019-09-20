from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from confluent_kafka import (
    TIMESTAMP_LOG_APPEND_TIME,
    Consumer as KafkaConsumer,
    TopicPartition,
)

from snuba.utils.kafka.consumer import Consumer


logger = logging.getLogger(__name__)


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
class Task:
    """Represents a single task to be executed."""


@dataclass(frozen=True)
class TaskSet:
    """
    Represents a collection of tasks to be executed.
    """

    topic: str
    partition: int
    interval: Interval
    tasks: Sequence[Task]


def get_boolean_configuration_value(
    configuration: Mapping[str, Any], key: str, default: bool
) -> bool:
    value = configuration.get(key, default)
    if isinstance(value, str):
        return value.lower().strip() == "true"
    elif isinstance(value, bool):
        return value
    else:
        raise TypeError(f"unexpected value for {key!r}")


class PartitionState:
    def __init__(self) -> None:
        self.__state: Union[None, Position, Interval] = None

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.__state}>"

    def send(self, position: Position) -> Union[None, Position, Interval]:
        if self.__state is None:
            self.__state = position
            return self.__state
        elif isinstance(self.__state, Position):
            self.__state = Interval(self.__state, position)
            return self.__state
        elif isinstance(self.__state, Interval):
            self.__state = self.__state.shift(position)
            return self.__state
        else:
            raise ValueError("unexpected state")


class TaskSetConsumer(Consumer[TaskSet]):
    """
    Identifies tasks to be executed based on the data within a Kafka topic.
    """

    def __init__(self, configuration: Mapping[str, Any]) -> None:
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
        self.__topic_partition_states: MutableMapping[
            Tuple[str, int], PartitionState
        ] = {}

        # Using the built-in ``enable.auto.offset.store`` behavior will cause
        # gaps in task execution during restarts or rebalances, since it will
        # set the next offset to be consumed as the most recently message
        # consumed's offset + 1. In our case, we always want the first message
        # to be consumed after a rebalance to be *the same* as the previously
        # consumed message. To account for this, we have to always disable it,
        # and use our own implementation that stores the offset of the most
        # recently received message in a partition, instead of that offset + 1.
        self.__consumer = KafkaConsumer(
            {**configuration, "enable.auto.offset.store": "false"}
        )

        # XXX: There's no way to deal with this right now and *not* store
        # offsets, so this option is effectively required.
        self.__enable_auto_offset_store = get_boolean_configuration_value(
            configuration, "enable.auto.offset.store", True
        )

    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TopicPartition]], Any]] = None,
        on_revoke: Optional[Callable[[Sequence[TopicPartition]], Any]] = None,
    ) -> None:
        def on_assign_callback(
            consumer: Any, assignment: Sequence[TopicPartition]
        ) -> None:
            for tp in assignment:
                key = (tp.topic, tp.partition)
                if key not in self.__topic_partition_states:
                    state = PartitionState()
                    self.__topic_partition_states[key] = state
                    logger.debug("Initialized partition state for %r: %r", key, state)

            if on_assign is not None:
                on_assign(assignment)

        def on_revoke_callback(
            consumer: Any, assignment: Sequence[TopicPartition]
        ) -> None:
            # XXX: Check to see if this causes any weird shit during rebalancing.
            for tp in assignment:
                key = (tp.topic, tp.partition)
                state = self.__topic_partition_states.pop(key)
                logger.debug("Discarded partition state for %r: %r", key, state)

            if on_revoke is not None:
                on_revoke(assignment)

        self.__consumer.subscribe(
            topics, on_assign=on_assign_callback, on_revoke=on_revoke_callback
        )

    def poll(self, timeout: Optional[float] = None) -> Optional[TaskSet]:
        message = self.__consumer.poll(*[timeout] if timeout is not None else [])
        if message is None:
            return None

        error = message.error()
        if error is not None:
            raise error

        timestamp_type, timestamp = message.timestamp()
        assert timestamp_type == TIMESTAMP_LOG_APPEND_TIME

        state = self.__topic_partition_states[
            (message.topic(), message.partition())
        ].send(Position(message.offset(), timestamp / 1000.0))

        if isinstance(state, Position):
            tasks = None
        elif isinstance(state, Interval):
            # TODO: Fetch tasks that are scheduled between the interval
            # endpoints for this topic and partition.
            tasks = TaskSet(message.topic(), message.partition(), state, [])
        else:
            raise ValueError("unexpected state")

        if self.__enable_auto_offset_store:
            self.__consumer.store_offsets(
                offsets=[
                    TopicPartition(
                        message.topic(), message.partition(), message.offset()
                    )
                ]
            )

        return tasks

    def commit(self, asynchronous: bool = True) -> Optional[Sequence[TopicPartition]]:
        offsets: Optional[Sequence[TopicPartition]] = self.__consumer.commit(
            asynchronous=asynchronous
        )
        return offsets

    def close(self) -> None:
        self.__consumer.close()
