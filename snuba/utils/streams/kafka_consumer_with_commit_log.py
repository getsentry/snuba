from datetime import datetime
from typing import Any, Mapping, Optional

from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.synchronized import Commit, commit_codec
from arroyo.types import Offset
from arroyo.utils.retries import RetryPolicy
from confluent_kafka import KafkaError
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import Producer as ConfluentProducer

from snuba.settings import PAYLOAD_DATETIME_FORMAT


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

    def commit_offsets(self) -> Mapping[Partition, Offset]:
        offsets = super().commit_offsets()

        for partition, offset in offsets.items():
            commit = Commit(self.__group_id, partition, offset.kafka_offset)
            payload = commit_codec.encode(commit)
            self.__producer.produce(
                self.__commit_log_topic.name,
                key=payload.key,
                value=payload.value,
                headers={
                    "orig_message_ts": datetime.strftime(
                        offset.timestamp, PAYLOAD_DATETIME_FORMAT
                    ),
                },
                on_delivery=self.__commit_message_delivery_callback,
            )

        return offsets

    def close(self, timeout: Optional[float] = None) -> None:
        super().close()
        messages: int = self.__producer.flush(*[timeout] if timeout is not None else [])
        if messages > 0:
            raise TimeoutError(f"{messages} commit log messages pending delivery")
