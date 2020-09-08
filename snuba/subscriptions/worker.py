from __future__ import annotations

import copy
import itertools
import json
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import timedelta
from typing import Mapping, NamedTuple, Optional, Sequence, Tuple

from snuba.datasets.dataset import Dataset
from snuba.reader import Result
from snuba.request import Request
from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.scheduler import ScheduledTask, Scheduler
from snuba.utils.codecs import Encoder
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.gauge import Gauge
from snuba.utils.metrics.timer import Timer
from snuba.utils.streams.abstract import Producer
from snuba.utils.streams.batching import AbstractBatchWorker
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Message, Topic
from snuba.web.query import parse_and_run_query


class SubscriptionTaskResultFuture(NamedTuple):
    task: ScheduledTask[Subscription]
    future: Future[Tuple[Request, Result]]


class SubscriptionTaskResult(NamedTuple):
    task: ScheduledTask[Subscription]
    result: Tuple[Request, Result]


class SubscriptionWorker(
    AbstractBatchWorker[Tick, Sequence[SubscriptionTaskResultFuture]]
):
    def __init__(
        self,
        dataset: Dataset,
        executor: ThreadPoolExecutor,
        schedulers: Mapping[int, Scheduler[Subscription]],
        producer: Producer[KafkaPayload],
        topic: Topic,
        metrics: MetricsBackend,
        time_shift: Optional[timedelta] = None,
    ) -> None:
        self.__dataset = dataset
        self.__executor = executor
        self.__schedulers = schedulers
        self.__producer = producer
        self.__topic = topic
        self.__metrics = metrics
        self.__time_shift = time_shift if time_shift is not None else timedelta()

        self.__concurrent_gauge = Gauge(self.__metrics, "executor.concurrent")

    def __execute(
        self, task: ScheduledTask[Subscription], tick: Tick
    ) -> Tuple[Request, Result]:
        # Measure the amount of time that took between this task being
        # scheduled and it beginning to execute.
        self.__metrics.timing(
            "executor.latency", (time.time() - task.timestamp.timestamp()) * 1000
        )

        # XXX: The ``query`` name is taken from the web views so that all query
        # performance metrics are reported to the same spot, regardless of
        # execution environment.
        timer = Timer("query")

        request = task.task.data.build_request(
            self.__dataset, task.timestamp, tick.offsets.upper, timer,
        )

        with self.__concurrent_gauge:
            # XXX: The ``extra`` is discarded from ``QueryResult`` since it is
            # not particularly useful in this context and duplicates data that
            # is already being published to the query log.
            # XXX: The ``request`` instance is copied when passed to
            # ``parse_and_run_query`` since it can/will be mutated during
            # processing.
            return (
                request,
                parse_and_run_query(
                    self.__dataset, copy.deepcopy(request), timer
                ).result,
            )

    def process_message(
        self, message: Message[Tick]
    ) -> Optional[Sequence[SubscriptionTaskResultFuture]]:
        # Schedule all of the queries (tasks) associated with this tick message
        # for (asynchronous) execution.
        # NOTE: There is no backpressure here (tasks will be submitted to the
        # executor as quickly as they are received from the broker) but there
        # *is* a concurrency limit on the executor. If a much larger number of
        # queries are scheduled to execute than there is capacity to execute,
        # waiting for them to complete during ``flush_batch`` may exceed the
        # consumer poll timeout (or session timeout during consumer
        # rebalancing) and cause the entire batch to be have to be replayed.
        tick = message.payload.time_shift(self.__time_shift)
        return [
            SubscriptionTaskResultFuture(
                task, self.__executor.submit(self.__execute, task, tick)
            )
            for task in self.__schedulers[message.partition.index].find(tick.timestamps)
        ]

    def flush_batch(
        self, batch: Sequence[Sequence[SubscriptionTaskResultFuture]]
    ) -> None:
        # Map over the batch, converting the ``SubscriptionResultFuture`` to a
        # ``SubscriptionResult``. This will block and wait for any unfinished
        # queries to complete, and raise any exceptions encountered during
        # query execution. Either the entire batch succeeds, or the entire
        # batch fails.
        results = [
            SubscriptionTaskResult(task, future.result())
            for task, future in itertools.chain.from_iterable(batch)
        ]

        # Produce all of the subscription results asynchronously and wait for
        # them to complete. Again, either the entire batch succeeds, or the
        # entire batch fails.
        for future in as_completed(
            [
                self.__producer.produce(
                    self.__topic, subscription_task_result_encoder.encode(result)
                )
                for result in results
            ]
        ):
            future.result()


class SubscriptionTaskResultEncoder(Encoder[KafkaPayload, SubscriptionTaskResult]):
    def encode(self, value: SubscriptionTaskResult) -> KafkaPayload:
        subscription_id = str(value.task.task.identifier)
        request, result = value.result
        return KafkaPayload(
            subscription_id.encode("utf-8"),
            json.dumps(
                {
                    "version": 2,
                    "payload": {
                        "subscription_id": subscription_id,
                        "request": {**request.body},
                        "result": result,
                        "timestamp": value.task.timestamp.isoformat(),
                    },
                }
            ).encode("utf-8"),
        )


subscription_task_result_encoder = SubscriptionTaskResultEncoder()
