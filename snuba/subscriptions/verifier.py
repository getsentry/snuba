from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import cached_property
from typing import Any, Callable, Mapping, MutableMapping, Optional, Sequence, Set, cast

from arroyo import Message, Partition, Topic
from arroyo.backends.abstract import Consumer
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import ProcessingStrategy
from arroyo.processing.strategies.abstract import ProcessingStrategyFactory
from arroyo.types import Position

from snuba.reader import Result
from snuba.utils.codecs import Decoder
from snuba.utils.metrics import MetricsBackend
from snuba.utils.streams.configuration_builder import build_kafka_consumer_configuration

logger = logging.getLogger(__name__)

COMMIT_FREQUENCY_SEC = 1


@dataclass(frozen=True)
class SubscriptionResultData:
    subscription_id: str
    request: Mapping[str, Any]
    result: Result
    timestamp: int
    entity_name: str

    @cached_property
    def identifier(self) -> str:
        return f"{self.entity_name}:{self.subscription_id}:{self.timestamp}"

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.to_dict() == cast(SubscriptionResultData, other).to_dict()
        )

    def is_off_by_one(self, other: SubscriptionResultData) -> bool:
        a = self.to_dict()
        b = other.to_dict()
        data_a = a.pop("data")
        data_b = b.pop("data")

        # Data is a list of dicts
        try:
            if len(data_a) == len(data_b):
                for (a, b) in zip(data_a, data_b):
                    a_keys = a.keys()
                    b_keys = b.keys()
                    if a_keys == b_keys:
                        for key in a_keys:
                            if abs(a[key] - b[key]) > 1:
                                return False
                return True
        except (TypeError, AttributeError):
            pass
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "data": self.result["data"],
            "meta": self.result["meta"],
            "totals": self.result.get("totals"),
            "request": self.request,
        }


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
    def __init__(
        self, consumer: Consumer[KafkaPayload], override_topics: Sequence[Topic]
    ):
        self.__consumer = consumer
        self.__decoder = SubscriptionResultDecoder()
        self.__override_topics = override_topics

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        # Dirty hack since StreamProcessor does not suport subscribing to multiple topics
        if self.__override_topics:
            topics = self.__override_topics

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


def build_verifier(
    orig_result_topic_name: str,
    new_result_topic_name: str,
    consumer_group: str,
    threshold_sec: int,
    auto_offset_reset: str,
    metrics: MetricsBackend,
) -> StreamProcessor[SubscriptionResultData]:
    orig_result_topic = Topic(orig_result_topic_name)
    new_result_topic = Topic(new_result_topic_name)

    return StreamProcessor(
        SubscriptionResultConsumer(
            KafkaConsumer(
                build_kafka_consumer_configuration(
                    None,
                    group_id=consumer_group,
                    auto_offset_reset=auto_offset_reset,
                )
            ),
            override_topics=[orig_result_topic, new_result_topic],
        ),
        # We have to pass a single topic here to keep the stream processor happy. But it's
        # ignored since we passed both of our topics to override_topics to the result consumer.
        orig_result_topic,
        VerifierProcessingFactory(
            orig_result_topic=orig_result_topic,
            new_result_topic=new_result_topic,
            threshold_sec=threshold_sec,
            metrics=metrics,
        ),
    )


class ResultTopic(Enum):
    ORIGINAL = "original"
    NEW = "new"


class ResultStore:
    """
    Keeps track of messages received on the old and new result topics grouped by
    their scheduled subscription time.
    """

    def __init__(self, threshold_sec: int, metrics: MetricsBackend) -> None:
        # For each result topic, stores all the subscription IDs and their
        # result checksums for each second.
        self.__stores: Mapping[
            ResultTopic,
            MutableMapping[int, MutableMapping[str, SubscriptionResultData]],
        ] = {
            ResultTopic.ORIGINAL: {},
            ResultTopic.NEW: {},
        }

        # The last timestamp we recorded metrics for
        # (or the time we will start recording)
        self.__timestamp_low_watermark: Optional[int] = None

        # The most recent timestamp we've seen
        self.__timestamp_high_watermark: Optional[int] = None

        self.__threshold_sec = threshold_sec
        self.__metrics = metrics

    def increment(self, topic: ResultTopic, item: SubscriptionResultData) -> None:
        # When we get the first message, set the low watermark
        if self.__timestamp_low_watermark is None:
            self.__timestamp_low_watermark = item.timestamp + self.__threshold_sec

        # Advance the high watermark if needed
        if (
            self.__timestamp_high_watermark is None
            or item.timestamp > self.__timestamp_high_watermark
        ):
            self.__timestamp_high_watermark = item.timestamp

        # Ensure the message is not below the low watermark
        if item.timestamp <= self.__timestamp_low_watermark:
            # Record `stale_message` unless the consumer is just starting up
            if (
                self.__timestamp_high_watermark
                >= self.__timestamp_low_watermark + self.__threshold_sec
            ):
                self.__metrics.increment("stale_message", tags={"topic": topic.value})
            return

        # Increment counters
        store = self.__stores[topic]

        if item.timestamp not in store:
            store[item.timestamp] = {}
        store[item.timestamp][item.identifier] = item

        # Record metrics and advance the low watermark
        low_watermark = self.__timestamp_high_watermark - self.__threshold_sec

        if low_watermark > self.__timestamp_low_watermark:
            self._record(low_watermark)
            self.__timestamp_low_watermark = low_watermark

    def _record(self, until: int) -> None:
        """
        Records metrics up to and including `until` then drop recorded elements.
        """
        while True:
            next_orig_ts = next(iter(self.__stores[ResultTopic.ORIGINAL]), None)
            next_new_ts = next(iter(self.__stores[ResultTopic.NEW]), None)

            if next_orig_ts is not None and next_orig_ts > until:
                next_orig_ts = None
            if next_new_ts is not None and next_new_ts > until:
                next_new_ts = None

            # Set next_ts
            if next_orig_ts is None and next_new_ts is None:
                next_ts = None
            elif next_orig_ts is None:
                next_ts = next_new_ts
            elif next_new_ts is None:
                next_ts = next_orig_ts
            else:
                next_ts = min([next_orig_ts, next_new_ts])

            # Nothing else to remove
            if next_ts is None:
                return

            orig_results = self.__stores[ResultTopic.ORIGINAL].pop(next_ts, {})
            new_results = self.__stores[ResultTopic.NEW].pop(next_ts, {})

            orig_subscription_ids = set(orig_results.keys())
            new_subscription_ids = set(new_results.keys())

            intersection_ids = new_subscription_ids.intersection(orig_subscription_ids)

            METRIC_OUTCOME_NAME = "subscription_result_outcomes"

            self.__metrics.increment(
                METRIC_OUTCOME_NAME,
                len(orig_subscription_ids - intersection_ids),
                tags={"outcome": "missing_from_new"},
            )
            self.__metrics.increment(
                METRIC_OUTCOME_NAME,
                len(new_subscription_ids - intersection_ids),
                tags={"outcome": "missing_from_orig"},
            )

            matching_results: Set[str] = set()

            for id in intersection_ids:
                if orig_results[id] == new_results[id]:
                    matching_results.add(id)

            non_matching_results = intersection_ids - matching_results

            # Group the non matching results into those that are off than one,
            # and those more different than off by one
            off_by_one: Set[str] = set()
            for id in non_matching_results:
                if new_results[id].is_off_by_one(orig_results[id]):
                    off_by_one.add(id)

            self.__metrics.increment(
                METRIC_OUTCOME_NAME,
                len(matching_results),
                {"outcome": "same_result"},
            )

            self.__metrics.increment(
                METRIC_OUTCOME_NAME,
                len(off_by_one),
                {"outcome": "off_by_one"},
            )

            self.__metrics.increment(
                METRIC_OUTCOME_NAME,
                len(non_matching_results) - len(off_by_one),
                {"outcome": "different_result"},
            )

            # Log up to 100 subscription results per second
            for id in list(non_matching_results - off_by_one)[:100]:
                logger.warning(
                    "Encountered non matching subscription result",
                    extra={
                        "subscription_id": id,
                        "original": orig_results[id].to_dict(),
                        "new": new_results[id].to_dict(),
                    },
                )


class CountResults(ProcessingStrategy[SubscriptionResultData]):
    """
    Records a metric with the result counts on the new vs original
    result topics.
    """

    def __init__(
        self,
        orig_result_topic: Topic,
        new_result_topic: Topic,
        threshold_sec: int,
        metrics: MetricsBackend,
        commit: Callable[[Mapping[Partition, Position]], None],
    ):
        self.__orig_result_topic = orig_result_topic
        self.__new_result_topic = new_result_topic
        self.__commit = commit
        self.__commit_data: MutableMapping[Partition, Position] = {}
        self.__last_committed: Optional[float] = None

        self.__store = ResultStore(threshold_sec, metrics)
        self.__closed = False

    def __throttled_commit(self, force: bool = False) -> None:
        # Commits all offsets and resets self.__commit_data at most
        # every COMMIT_FREQUENCY_SEC. If force=True is passed, the
        # commit frequency is ignored and we immediately commit.

        now = time.time()

        if (
            self.__last_committed is None
            or now - self.__last_committed >= COMMIT_FREQUENCY_SEC
            or force is True
        ):
            self.__commit(self.__commit_data)
            self.__last_committed = now
            self.__commit_data = {}

    def poll(self) -> None:
        pass

    def submit(self, message: Message[SubscriptionResultData]) -> None:
        assert not self.__closed

        message_topic = message.partition.topic
        store_key = {
            self.__orig_result_topic: ResultTopic.ORIGINAL,
            self.__new_result_topic: ResultTopic.NEW,
        }[message_topic]

        self.__store.increment(store_key, message.payload)

        self.__commit_data[message.partition] = Position(
            message.offset, message.timestamp
        )
        self.__throttled_commit()

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        self.__throttled_commit(force=True)


class VerifierProcessingFactory(ProcessingStrategyFactory[SubscriptionResultData]):
    def __init__(
        self,
        orig_result_topic: Topic,
        new_result_topic: Topic,
        threshold_sec: int,
        metrics: MetricsBackend,
    ):
        self.__orig_result_topic = orig_result_topic
        self.__new_result_topic = new_result_topic
        self.__threshold_sec = threshold_sec
        self.__metrics = metrics

    def create(
        self, commit: Callable[[Mapping[Partition, Position]], None]
    ) -> ProcessingStrategy[SubscriptionResultData]:
        return CountResults(
            orig_result_topic=self.__orig_result_topic,
            new_result_topic=self.__new_result_topic,
            threshold_sec=self.__threshold_sec,
            metrics=self.__metrics,
            commit=commit,
        )
