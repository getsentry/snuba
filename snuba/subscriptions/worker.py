from __future__ import annotations

import itertools
import json
from concurrent.futures import Future, as_completed
from typing import Mapping, NamedTuple, Optional, Sequence

from snuba.reader import Result
from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.executor import SubscriptionExecutor
from snuba.subscriptions.scheduler import ScheduledTask, Scheduler
from snuba.utils.codecs import Codec
from snuba.utils.streams.batching import AbstractBatchWorker
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.producer import Producer
from snuba.utils.streams.types import Message, Topic


class SubscriptionResultFuture(NamedTuple):
    task: ScheduledTask[Subscription]
    future: Future[Result]


class SubscriptionResult(NamedTuple):
    task: ScheduledTask[Subscription]
    result: Result


class SubscriptionWorker(AbstractBatchWorker[Tick, Sequence[SubscriptionResultFuture]]):
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
    ) -> Optional[Sequence[SubscriptionResultFuture]]:
        # Schedule all of the queries (tasks) associated with this tick message
        # for (asynchronous) execution.
        # NOTE: There is no backpressure here (tasks will be submitted to the
        # executor as quickly as they are received from the broker) but there
        # *is* a concurrency limit on the executor. If a much larger number of
        # queries are scheduled to execute than there is capacity to execute,
        # waiting for them to complete during ``flush_batch`` may exceed the
        # consumer poll timeout (or session timeout during consumer
        # rebalancing) and cause the entire batch to be have to be replayed.
        tick = message.payload
        return [
            SubscriptionResultFuture(task, self.__executor.execute(task, tick))
            for task in self.__schedulers[message.partition.index].find(tick.timestamps)
        ]

    def flush_batch(self, batch: Sequence[Sequence[SubscriptionResultFuture]]) -> None:
        # Map over the batch, converting the ``SubscriptionResultFuture`` to a
        # ``SubscriptionResult``. This will block and wait for any unfinished
        # queries to complete, and raise any exceptions encountered during
        # query execution. Either the entire batch succeeds, or the entire
        # batch fails.
        results = [
            SubscriptionResult(task, future.result())
            for task, future in itertools.chain.from_iterable(batch)
        ]

        # Produce all of the subscription results asynchronously and wait for
        # them to complete. Again, either the entire batch succeeds, or the
        # entire batch fails.
        for future in as_completed(
            [self.__producer.produce(self.__topic, result) for result in results]
        ):
            future.result()


class SubscriptionResultCodec(Codec[KafkaPayload, SubscriptionResult]):
    def encode(self, value: SubscriptionResult) -> KafkaPayload:
        subscription_id = str(value.task.task.identifier)
        return KafkaPayload(
            subscription_id.encode("utf-8"),
            json.dumps(
                {
                    "version": 1,
                    "payload": {
                        "subscription_id": subscription_id,
                        "values": value.result,
                        "timestamp": value.task.timestamp.isoformat(),
                    },
                }
            ).encode("utf-8"),
        )

    def decode(self, value: KafkaPayload) -> SubscriptionResult:
        raise NotImplementedError
