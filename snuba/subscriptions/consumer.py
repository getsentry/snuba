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

from snuba.utils.kafka.configuration import get_bool_configuration_value
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
        if not self.upper.offset > self.lower.offset:
            raise ValueError('upper offset must be greater than lower offset')

        if not self.upper.timestamp >= self.lower.timestamp:
            raise ValueError('upper timestamp must be greater than or equal to lower timestamp')

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


class PartitionState:
    def __init__(self) -> None:
        # There are three valid states for a partition in this structure:
        # 1. Partitions that have been assigned but have not yet had any
        #    messages consumed from them will have a value of ``None``.
        # 2. Partitions that have had a single message consumed will have a
        #    value of ``Position``.
        # 3. Partitions that have had more than one message consumed will have
        #    a value of ``Interval``.
        self.__state: Union[None, Position, Interval] = None

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.__state}>"

    def set_position(self, position: Position) -> Union[None, Interval]:
        """
        Set the position of this partition, returning the interval between
        the current position and previous position, or ``None`` if no prior
        position was set.
        """
        if self.__state is None:
            self.__state = position
            return None
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
    A ``Consumer`` class that implements an API that is similar to the
    ``Message``-based API, except that it returns a ``TaskSet`` instance
    rather than a Kafka ``Message``.

    This consumer uses the progression of time in a Kafka partition as a
    "virtual clock" rather than depending on wall clock time. This allows
    replaying history without gaps in task execution if the consumer
    is not running when a task would have been scheduled to execute for
    increased reliability (no schedule gaps) and easier testing on historical
    data.

    This consumer differs from a typical consumer in the way that messages
    are processed: the tasks that are contained within a task set are
    identified by whether or not they fall within a time interval formed by
    the timestamps of the last two messages received within a partition.

    The first message received will not cause a `TaskSet` to be returned by
    the consumer, since only one endpoint of the interval can be established.

    The second message received by the consumer will cause a `TaskSet` to be
    created with all tasks scheduled between the time interval represented by
    the timestamps of the previous two messages consumed within that
    partition. (This interval's lower bound is exclusive, and the upper bound
    is inclusive.) All subsequent messages recieved by the consumer will
    similarly cause a ``TaskSet`` to be created with all tasks scheduled
    between the timestamps of the previous two messages consumed within that
    partition as well.

    This consumer requires that the ``message.timestamp.type`` topic
    configuration is set to ``LogAppendTime`` for the topics that it is
    subscribed to, so that that the message time is set by the primary broker
    for the topic -- not the producer of the message -- ensuring that each
    partition timestamp moves monotonically.

    This consumer can either be used with automatic timed offset commit (the
    librdkafka default, set using the ``enable.auto.commit`` configuration
    value) or manual calls to ``commit``.
    """

    # Since this consumer deals with time intervals between messages rather
    # individual messages, this introduces some additional complexity into the
    # way that offsets are managed. Take this example, where a partition
    # contains three messages (MA, MB, MC) and a repeating scheduled task
    # (T1-TN).
    #
    #    Messages:           MA        MB        MC
    #    Offset:             0         1         2
    #    Timeline: +---------+---------+---------+---------
    #    Tasks:    ^    ^    ^    ^    ^    ^    ^    ^
    #              T1   T2   T3   T4   T5   T6   T7   T8
    #
    # Consuming Message A ("MA") does not cause any tasks to be dispatched,
    # since we cannot form an interval with a single message. When Message B
    # ("MB") is consumed, we can form a time interval ``(MA, MB)`` from the
    # timestamps of the the two messages. The task set will contain the tasks
    # T4 and T5. T3 will not be included: since it has a timestamp equal to the
    # timestamp of MA, it is associated with the task set that has the upper
    # bound of MA. (Remember, our lower bound is exclusive, but our upper bound
    # is inclusive.)
    #
    # When storing (or committing) offsets, we also need to be careful that we
    # commit the offsets/message representing the correct interval endpoint so
    # that intervals are not repeated or skipped during restarts or rebalance
    # operations. After a task set (or batch of task sets) have been executed,
    # committing the task set should ensure that tasks already received are not
    # returned by any subsequent ``poll`` calls, and the next task set returned
    # by a ``poll`` call on this partition should have a lower bound endpoint
    # that is equal to the upper bound of the task set we just committed. Using
    # the previous example's interval of ``(MA, MB)``, committing the offset
    # ``1`` will cause MB to be the first message consumed by a consumer that
    # has just been assigned this partition. The first interval formed by that
    # consumer will be ``(MB, MC)``, with a corresponding task set returned
    # that contains both T6 and T7.

    def __init__(self, configuration: Mapping[str, Any]) -> None:
        self.__topic_partition_states: MutableMapping[
            Tuple[str, int], PartitionState
        ] = {}

        # Using the built-in ``enable.auto.offset.store`` behavior will cause
        # gaps in task execution during restarts or rebalances, since it sets
        # the next offset to be consumed as the most recently message
        # consumed's offset + 1. In our case (as described above), we always
        # want the first message to be consumed after a rebalance to be *the
        # same* as the most recently consumed message. To account for this
        # discrepancy, we disable the default automatic offset storage
        # behavior, and use our own implementation that stores the offset of
        # the most recently received message in a partition, instead of that
        # messages's offset + 1.
        self.__consumer = KafkaConsumer(
            {**configuration, "enable.auto.offset.store": "false"}
        )

        # XXX: The only way to store offsets (and correspondingly, cause those
        # stored offsets to be committed) is to use the automatic offset
        # storage. (There is no ``store_offsets`` method.) That makes this
        # configuration value effectively required
        self.__enable_auto_offset_store = get_bool_configuration_value(
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
        # NOTE: The ``Consumer`` implementation only accepts real numbers as
        # the timeout, not ``None``. There is no value that represents "no
        # timeout", other than the lack of any argument at all.
        message = self.__consumer.poll(*[timeout] if timeout is not None else [])
        if message is None:
            return None

        error = message.error()
        if error is not None:
            raise error

        timestamp_type, timestamp = message.timestamp()
        assert timestamp_type == TIMESTAMP_LOG_APPEND_TIME

        interval = self.__topic_partition_states[
            (message.topic(), message.partition())
        ].set_position(Position(message.offset(), timestamp / 1000.0))

        if interval is not None:
            # TODO: Fetch tasks that are scheduled between the interval
            # endpoints for this topic and partition.
            tasks: Optional[TaskSet] = TaskSet(
                message.topic(), message.partition(), interval, []
            )
        else:
            tasks = None

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
