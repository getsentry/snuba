import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping, Optional, Sequence

from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Consumer
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import Position

from snuba.reader import Result
from snuba.utils.codecs import Decoder

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SubscriptionResultData:
    subscription_id: str
    request: Mapping[str, Any]
    result: Result
    timestamp: int
    entity_name: str


class SubscriptionResultDecoder(Decoder[KafkaPayload, SubscriptionResultData]):
    def decode(self, value: KafkaPayload) -> SubscriptionResultData:
        body = value.value
        data = json.loads(body.decode("utf-8"))
        payload = data["payload"]
        return SubscriptionResultData(
            payload["subscription_id"],
            payload["request"],
            payload["result"],
            int(datetime.fromisoformat(payload["timestamp"]).timestamp()),
            payload["entity"],
        )


class SubscriptionResultConsumer(Consumer[SubscriptionResultData]):
    def __init__(self, consumer: Consumer[KafkaPayload]):
        self.__consumer = consumer
        self.__decoder = SubscriptionResultDecoder()

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        self.__consumer.subscribe(topics, on_assign=on_assign, on_revoke=on_revoke)

    def unsubscribe(self) -> None:
        self.__consumer.unsubscribe()

    def poll(
        self, timeout: Optional[float] = None
    ) -> Optional[Message[SubscriptionResultData]]:
        kafka_message = self.__consumer.poll(timeout)

        if kafka_message is None:
            return None

        decoded = self.__decoder.decode(kafka_message.payload)

        return Message(
            kafka_message.partition,
            kafka_message.offset,
            decoded,
            kafka_message.timestamp,
            kafka_message.next_offset,
        )

    def pause(self, partitions: Sequence[Partition]) -> None:
        self.__consumer.pause(partitions)

    def resume(self, partitions: Sequence[Partition]) -> None:
        self.__consumer.resume(partitions)

    def paused(self) -> Sequence[Partition]:
        return self.__consumer.paused()

    def tell(self) -> Mapping[Partition, int]:
        return self.__consumer.tell()

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        self.__consumer.seek(offsets)

    def stage_positions(self, positions: Mapping[Partition, Position]) -> None:
        return self.__consumer.stage_positions(positions)

    def commit_positions(self) -> Mapping[Partition, Position]:
        return self.__consumer.commit_positions()

    def close(self, timeout: Optional[float] = None) -> None:
        return self.__consumer.close(timeout)

    @property
    def closed(self) -> bool:
        return self.__consumer.closed
