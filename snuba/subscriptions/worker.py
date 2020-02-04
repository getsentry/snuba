from __future__ import annotations

import json
from typing import Mapping, MutableSequence, Optional, Sequence, NamedTuple

from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.executor import SubscriptionExecutor
from snuba.subscriptions.scheduler import Scheduler, ScheduledTask
from snuba.utils.codecs import Codec
from snuba.utils.streams.batching import AbstractBatchWorker
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.producer import Producer
from snuba.utils.streams.types import Message, Topic
from snuba.web.query import ClickhouseQueryResult


class SubscriptionResult(NamedTuple):
    task: ScheduledTask[Subscription]
    result: ClickhouseQueryResult


class SubscriptionWorker(AbstractBatchWorker[Tick, Sequence[SubscriptionResult]]):
    def __init__(
        self,
        executor: SubscriptionExecutor,
        schedulers: Mapping[int, Scheduler[Subscription]],
        producer: Producer[SubscriptionResult],
        topic: Topic,
    ) -> None:
        self.__executor = executor
        self.__schedulers = schedulers
        self.__producer = producer
        self.__topic = topic

    def process_message(
        self, message: Message[Tick]
    ) -> Optional[Sequence[SubscriptionResult]]:
        results: MutableSequence[SubscriptionResult] = []

        tick = message.payload
        for task in self.__schedulers[message.partition.index].find(tick.timestamps):
            results.append(
                SubscriptionResult(task, self.__executor.execute(task, tick).result())
            )

        return results

    def flush_batch(self, batch: Sequence[Sequence[SubscriptionResult]]) -> None:
        for results in batch:
            for result in results:
                self.__producer.produce(self.__topic, result).result()


class SubscriptionResultCodec(Codec[KafkaPayload, SubscriptionResult]):
    def encode(self, value: SubscriptionResult) -> KafkaPayload:
        subscription_id = str(value.task.task.identifier)
        return KafkaPayload(
            str(subscription_id).encode("utf-8"),
            json.dumps(
                {
                    "version": 1,
                    "payload": {
                        "subscription_id": str(subscription_id),
                        "values": value.result["data"],
                        "timestamp": value.task.timestamp.timestamp(),
                    },
                }
            ).encode("utf-8"),
        )

    def decode(self, value: KafkaPayload) -> SubscriptionResult:
        raise NotImplementedError
