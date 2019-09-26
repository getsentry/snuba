from typing import Any, Callable, Mapping, Optional, Sequence

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import Message, TopicPartition

from snuba.utils.kafka.consumers.abstract import Consumer as AbstractConsumer


class Consumer(AbstractConsumer[Message]):
    def __init__(self, configuration: Mapping[str, Any]) -> None:
        self.__consumer = ConfluentConsumer(configuration)

    def __wrap_assignment_callback(
        self, callback: Callable[[Sequence[TopicPartition]], Any]
    ) -> Callable[[Any, Sequence[TopicPartition]], None]:
        def wrapper(consumer: Any, assignment: Sequence[TopicPartition]) -> None:
            callback(assignment)

        return wrapper

    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TopicPartition]], Any]] = None,
        on_revoke: Optional[Callable[[Sequence[TopicPartition]], Any]] = None,
    ) -> None:
        kwargs = {}

        if on_assign is not None:
            kwargs["on_assign"] = self.__wrap_assignment_callback(on_assign)

        if on_revoke is not None:
            kwargs["on_revoke"] = self.__wrap_assignment_callback(on_revoke)

        self.__consumer.subscribe(topics, **kwargs)

    def poll(self, timeout: Optional[float] = None) -> Optional[Message]:
        return self.__consumer.poll(*[timeout] if timeout is not None else [])

    def commit(self, asynchronous: bool = True) -> Optional[Sequence[TopicPartition]]:
        offsets: Optional[Sequence[TopicPartition]] = self.__consumer.commit(
            asynchronous=asynchronous
        )
        return offsets

    def close(self) -> None:
        self.__consumer.close()
