import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
)

from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Consumer
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.types import Position

from snuba.reader import Result
from snuba.utils.codecs import Decoder
from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration

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


class SynchronizedConsumer(Consumer[SubscriptionResultData]):
    """
    This is a consumer that attempts to synchronize progress between the old and the
    new result topics based on the scheduled subscription time so that we can make
    progress on both at roughly the same pace.

    IMPORTANT: The messages returned by the poll method of this consumer are from BOTH
    topics interchangeably. Use with caution!
    """

    def __init__(
        self,
        orig_topic: Topic,
        new_topic: Topic,
        # The same consumer group name is used on both old and new topics
        consumer_group: str,
        auto_offset_reset: str,
    ) -> None:
        self.__orig_topic_consumer = self._build_result_consumer(
            consumer_group, auto_offset_reset
        )
        self.__new_topic_consumer = self._build_result_consumer(
            consumer_group, auto_offset_reset
        )
        self.__orig_topic = orig_topic
        self.__new_topic = new_topic

        self.__orig_consumer_last_message: Optional[
            Message[SubscriptionResultData]
        ] = None
        self.__new_consumer_last_message: Optional[
            Message[SubscriptionResultData]
        ] = None

    def _build_result_consumer(
        self, consumer_group: str, auto_offset_reset: str
    ) -> Consumer[SubscriptionResultData]:
        return SubscriptionResultConsumer(
            KafkaConsumer(
                build_kafka_consumer_configuration(
                    None, group_id=consumer_group, auto_offset_reset=auto_offset_reset,
                ),
            )
        )

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        """
        Note the topic actually passed here is ignored as each consumer subscribes to a different topic
        """
        self.__orig_topic_consumer.subscribe(
            [self.__orig_topic], on_assign=on_assign, on_revoke=on_revoke
        )
        self.__new_topic_consumer.subscribe(
            [self.__new_topic], on_assign=on_assign, on_revoke=on_revoke
        )

    def unsubscribe(self) -> None:
        self.__orig_topic_consumer.unsubscribe()
        self.__new_topic_consumer.unsubscribe()

    def poll(
        self, timeout: Optional[float] = None
    ) -> Optional[Message[SubscriptionResultData]]:
        if self.__orig_consumer_last_message is None:
            self.__orig_consumer_last_message = self.__orig_topic_consumer.poll(timeout)

        if self.__new_consumer_last_message is None:
            self.__new_consumer_last_message = self.__new_topic_consumer.poll(timeout)

        message: Optional[Message[SubscriptionResultData]] = None

        if (
            self.__orig_consumer_last_message is None
            and self.__new_consumer_last_message is None
        ):
            pass
        elif self.__new_consumer_last_message is None:
            message = self.__orig_consumer_last_message
            self.__orig_consumer_last_message = None
        elif self.__orig_consumer_last_message is None:
            message = self.__new_consumer_last_message
            self.__new_consumer_last_message = None
        else:
            # Return the earlier message based on scheduled timestamp
            # (or the new topic message if both are the same)
            orig_payload = self.__orig_consumer_last_message.payload
            new_payload = self.__new_consumer_last_message.payload

            if orig_payload.timestamp < new_payload.timestamp:
                message = self.__orig_consumer_last_message
                self.__orig_consumer_last_message = None
            else:
                message = self.__new_consumer_last_message
                self.__new_consumer_last_message = None

        return message

    def __segment_partitions(
        self, partitions: Sequence[Partition]
    ) -> Tuple[Sequence[Partition], Sequence[Partition]]:
        """
        Partitions the partitions by topic and returns (orig_partitions, new_partitions).
        Raises an error if any partitions don't belong to one of the two topics.
        """
        orig_partitions: MutableSequence[Partition] = []
        new_partitions: MutableSequence[Partition] = []

        for partition in partitions:
            if partition.topic == self.__orig_topic:
                orig_partitions.append(partition)
            elif partition.topic == self.__new_topic:
                new_partitions.append(partition)
            else:
                raise Exception("invalid topic")

        return (orig_partitions, new_partitions)

    def __segment_offsets(
        self, offsets: Mapping[Partition, int]
    ) -> Tuple[Mapping[Partition, int], Mapping[Partition, int]]:
        orig_offsets: MutableMapping[Partition, int] = {}
        new_offsets: MutableMapping[Partition, int] = {}

        for partition in offsets.keys():
            if partition.topic == self.__orig_topic:
                orig_offsets[partition] = offsets[partition]
            elif partition.topic == self.__new_topic:
                new_offsets[partition] = offsets[partition]
            else:
                raise Exception("invalid topic")

        return (orig_offsets, new_offsets)

    def __segment_positions(
        self, positions: Mapping[Partition, Position]
    ) -> Tuple[Mapping[Partition, Position], Mapping[Partition, Position]]:
        orig_positions: MutableMapping[Partition, Position] = {}
        new_positions: MutableMapping[Partition, Position] = {}

        for partition in positions.keys():
            if partition.topic == self.__orig_topic:
                orig_positions[partition] = positions[partition]
            elif partition.topic == self.__new_topic:
                new_positions[partition] = positions[partition]
            else:
                raise Exception("invalid topic")

        return (orig_positions, new_positions)

    def pause(self, partitions: Sequence[Partition]) -> None:
        orig_partitions, new_partitions = self.__segment_partitions(partitions)

        self.__orig_topic_consumer.pause(orig_partitions)
        self.__new_topic_consumer.pause(new_partitions)

    def resume(self, partitions: Sequence[Partition]) -> None:
        orig_partitions, new_partitions = self.__segment_partitions(partitions)

        self.__orig_topic_consumer.resume(orig_partitions)
        self.__new_topic_consumer.resume(new_partitions)

    def paused(self) -> Sequence[Partition]:
        return [
            *self.__orig_topic_consumer.paused(),
            *self.__new_topic_consumer.paused(),
        ]

    def tell(self) -> Mapping[Partition, int]:
        return {**self.__orig_topic_consumer.tell(), **self.__new_topic_consumer.tell()}

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        orig_offsets, new_offsets = self.__segment_offsets(offsets)
        self.__orig_topic_consumer.seek(orig_offsets)
        self.__new_topic_consumer.seek(new_offsets)

    def stage_positions(self, positions: Mapping[Partition, Position]) -> None:
        orig_positions, new_positions = self.__segment_positions(positions)
        self.__orig_topic_consumer.stage_positions(orig_positions)
        self.__new_topic_consumer.stage_positions(new_positions)

    def commit_positions(self) -> Mapping[Partition, Position]:
        orig_positions = self.__orig_topic_consumer.commit_positions()
        new_positions = self.__new_topic_consumer.commit_positions()
        return {**orig_positions, **new_positions}

    def close(self, timeout: Optional[float] = None) -> None:
        self.__orig_topic_consumer.close(timeout)
        self.__new_topic_consumer.close(timeout)

    @property
    def closed(self) -> bool:
        return self.__orig_topic_consumer.closed and self.__new_topic_consumer.closed
