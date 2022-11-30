from typing import Any, Mapping, Optional

from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.backends.kafka.commit import CommitCodec
from arroyo.commit import Commit
from arroyo.types import BrokerValue, Partition, Position, Topic
from arroyo.utils.retries import RetryPolicy
from confluent_kafka import KafkaError
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import Producer as ConfluentProducer

commit_codec = CommitCodec()


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

    def poll(
        self, timeout: Optional[float] = None
    ) -> Optional[BrokerValue[KafkaPayload]]:
        self.__producer.poll(0.0)
        return super().poll(timeout)

    def __commit_message_delivery_callback(
        self, error: Optional[KafkaError], message: ConfluentMessage
    ) -> None:
        if error is not None:
            raise Exception(error.str())

    def commit_positions(self) -> Mapping[Partition, Position]:
        positions = super().commit_positions()

        for partition, position in positions.items():
            commit = Commit(
                self.__group_id, partition, position.offset, position.timestamp
            )
            payload = commit_codec.encode(commit)

            self.__producer.produce(
                self.__commit_log_topic.name,
                key=payload.key,
                value=payload.value,
                headers=payload.headers,
                on_delivery=self.__commit_message_delivery_callback,
            )

        return positions

    def close(self, timeout: Optional[float] = None) -> None:
        super().close()
        messages: int = self.__producer.flush(*[timeout] if timeout is not None else [])
        if messages > 0:
            raise TimeoutError(f"{messages} commit log messages pending delivery")
