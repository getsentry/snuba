from typing import Any, Callable, Mapping, Optional, Sequence

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import Message, TopicPartition

from snuba.utils.kafka.consumers.abstract import (
    Consumer as AbstractConsumer,
    Offset,
    TopicPartitionKey,
)


class Consumer(AbstractConsumer[Message]):
    """
    This consumer implements the abstract Consumer API by wrapping the
    Confluent Kafka client, modifying parameter and return values (including
    callbacks) as needed.
    """

    def __init__(self, configuration: Mapping[str, Any]) -> None:
        self.__consumer = ConfluentConsumer(configuration)

    def __wrap_assignment_callback(
        self, callback: Callable[[Sequence[TopicPartitionKey]], None]
    ) -> Callable[[ConfluentConsumer, Sequence[TopicPartition]], None]:
        """
        Wraps a partition assignment callback with a wrapper function that
        translates the Confluent Kafka consumer callback signature into the
        arguments expected by the callback.
        """

        def wrapper(consumer: Consumer, assignment: Sequence[TopicPartition]) -> None:
            callback([(TopicPartitionKey(i.topic, i.partition)) for i in assignment])

        return wrapper

    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TopicPartitionKey]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TopicPartitionKey]], None]] = None,
    ) -> None:
        kwargs = {}

        if on_assign is not None:
            kwargs["on_assign"] = self.__wrap_assignment_callback(on_assign)

        if on_revoke is not None:
            kwargs["on_revoke"] = self.__wrap_assignment_callback(on_revoke)

        self.__consumer.subscribe(topics, **kwargs)

    def poll(self, timeout: Optional[float] = None) -> Optional[Message]:
        return self.__consumer.poll(*[timeout] if timeout is not None else [])

    def commit(
        self, asynchronous: bool = True
    ) -> Optional[Mapping[TopicPartitionKey, Offset]]:
        offsets: Optional[Sequence[TopicPartition]] = self.__consumer.commit(
            asynchronous=asynchronous
        )
        if offsets is not None:
            return {
                TopicPartitionKey(i.topic, i.partition): Offset(i.offset)
                for i in offsets
            }
        else:
            return None

    def close(self) -> None:
        self.__consumer.close()
