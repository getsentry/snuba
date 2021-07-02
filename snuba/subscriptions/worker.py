from __future__ import annotations

import copy
import itertools
import logging
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import List, Mapping, NamedTuple, Optional, Sequence, Tuple

from arroyo import Message, Topic
from arroyo.backends.abstract import Producer
from arroyo.processing.strategies.batching import AbstractBatchWorker

from snuba import state
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.reader import Result
from snuba.request import Request
from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import DelegateSubscriptionData, Subscription
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.gauge import Gauge, ThreadSafeGauge
from snuba.utils.metrics.timer import Timer
from snuba.utils.scheduler import ScheduledTask, Scheduler
from snuba.utils.threaded_function_delegator import (
    Result as ThreadedFunctionDelegatorResult,
)
from snuba.utils.threaded_function_delegator import ThreadedFunctionDelegator
from snuba.web.query import parse_and_run_query

logger = logging.getLogger("snuba.subscriptions")


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
        producer: Producer[SubscriptionTaskResult],
        topic: Topic,
        metrics: MetricsBackend,
    ) -> None:
        self.__dataset = dataset
        self.__dataset_name = get_dataset_name(self.__dataset)
        self.__executor = executor
        self.__schedulers = schedulers
        self.__producer = producer
        self.__topic = topic
        self.__metrics = metrics

        self.__concurrent_gauge: Gauge = ThreadSafeGauge(
            self.__metrics, "executor.concurrent"
        )

        self.__concurrent_clickhouse_gauge: Gauge = ThreadSafeGauge(
            self.__metrics, "executor.concurrent.clickhouse"
        )

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

        if isinstance(task.task.data, DelegateSubscriptionData):
            try:
                request = task.task.data.build_request(
                    self.__dataset,
                    task.timestamp,
                    tick.offsets.upper,
                    timer,
                    self.__metrics,
                )
                return self.__execute_query(request, timer)
            except Exception as e:
                self.__metrics.increment("snql.subscription.delegate.error.execution")
                logger.warning(
                    f"failed snql subscription query: {e}",
                    exc_info=e,
                    extra={"error": str(e), "data": task.task.data.to_dict()},
                )
                request = task.task.data.to_legacy().build_request(
                    self.__dataset,
                    task.timestamp,
                    tick.offsets.upper,
                    timer,
                    self.__metrics,
                )
                return self.__execute_query(request, timer)

        request = task.task.data.build_request(
            self.__dataset, task.timestamp, tick.offsets.upper, timer, self.__metrics
        )
        return self.__execute_query(request, timer)

    def __execute_query(self, request: Request, timer: Timer) -> Tuple[Request, Result]:
        with self.__concurrent_gauge:
            is_consistent_query = request.settings.get_consistent()

            def run_consistent() -> Result:
                request_copy = copy.deepcopy(request)
                request_copy.settings.set_consistent(True)

                return parse_and_run_query(
                    self.__dataset,
                    request_copy,
                    timer,
                    robust=True,
                    concurrent_queries_gauge=self.__concurrent_clickhouse_gauge,
                ).result

            def run_non_consistent() -> Result:
                request_copy = copy.deepcopy(request)
                request_copy.settings.set_consistent(False)

                return parse_and_run_query(
                    self.__dataset,
                    copy.deepcopy(request),
                    timer,
                    robust=True,
                    concurrent_queries_gauge=self.__concurrent_clickhouse_gauge,
                ).result

            def selector_func(run_snuplicator: bool) -> Tuple[str, List[str]]:
                primary = "consistent" if is_consistent_query else "non_consistent"
                other = "non_consistent" if is_consistent_query else "consistent"

                return (primary, [other] if run_snuplicator else [])

            def callback_func(
                data: List[ThreadedFunctionDelegatorResult[Result]],
            ) -> None:
                primary_result = data.pop(0).result

                for result in data:
                    match = result.result == primary_result
                    self.__metrics.increment(
                        "consistent",
                        tags={
                            "dataset": self.__dataset_name,
                            "consistent": str(is_consistent_query),
                            "match": str(match),
                        },
                    )

            if self.__dataset_name == "events":
                sample_rate = state.get_config(
                    "event_subscription_non_consistent_sample_rate", 0
                )
            elif self.__dataset_name == "transactions":
                sample_rate = state.get_config(
                    "transaction_subscription_non_consistent_sample_rate", 0
                )
            else:
                sample_rate = 0

            assert sample_rate is not None

            run_snuplicator = random.random() < float(sample_rate)

            executor = ThreadedFunctionDelegator[bool, Result](
                callables={
                    "consistent": run_consistent,
                    "non_consistent": run_non_consistent,
                },
                selector_func=selector_func,
                callback_func=callback_func,
            )

            return (request, executor.execute(run_snuplicator))

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
        tick = message.payload
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
            [self.__producer.produce(self.__topic, result) for result in results]
        ):
            future.result()
