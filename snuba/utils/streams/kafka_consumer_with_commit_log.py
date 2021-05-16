from typing import Any, Mapping, Optional

from confluent_kafka import KafkaError
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import Producer as ConfluentProducer

from snuba.utils.retries import RetryPolicy
from snuba.utils.streams.backends.kafka import KafkaConsumer, KafkaPayload
from snuba.utils.streams.synchronized import Commit, commit_codec
from snuba.utils.streams.types import Message, Partition, Topic


class KafkaConsumerWithCommitLog(KafkaConsumer):
    def __init__(
        self,
        configuration: Mapping[str, Any],
        *,
        producer: ConfluentProducer,
        commit_log_topic: Topic,
        commit_retry_policy: Optional[RetryPolicy] = None,
    ) -> None:
        super().__init__(configuration, commit_retry_policy=commit_retry_policy)
        self.__producer = producer
        self.__commit_log_topic = commit_log_topic
        self.__group_id = configuration["group.id"]

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[KafkaPayload]]:
        self.__producer.poll(0.0)
        return super().poll(timeout)

    def __commit_message_delivery_callback(
        self, error: Optional[KafkaError], message: ConfluentMessage
    ) -> None:
        if error is not None:
            raise Exception(error.str())

    def commit_offsets(self) -> Mapping[Partition, int]:
        offsets = super().commit_offsets()

        for partition, offset in offsets.items():
            commit = Commit(self.__group_id, partition, offset)
            payload = commit_codec.encode(commit)
            self.__producer.produce(
                self.__commit_log_topic.name,
                key=payload.key,
                value=payload.value,
                on_delivery=self.__commit_message_delivery_callback,
            )

        return offsets

    def close(self, timeout: Optional[float] = None) -> None:
        super().close()
        messages: int = self.__producer.flush(*[timeout] if timeout is not None else [])
        if messages > 0:
            raise TimeoutError(f"{messages} commit log messages pending delivery")
