from __future__ import annotations

import copy
import itertools
import logging
import math
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Mapping, Optional, Sequence, Tuple

from arroyo import Message, Topic
from arroyo.backends.abstract import Producer
from arroyo.processing.strategies.batching import AbstractBatchWorker

from snuba import state
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.reader import Result
from snuba.request import Request
from snuba.request.request_settings import SubscriptionRequestSettings
from snuba.subscriptions.data import (
    ScheduledSubscriptionTask,
    SubscriptionScheduler,
    SubscriptionTaskResult,
    SubscriptionTaskResultFuture,
)
from snuba.subscriptions.utils import Tick
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.gauge import Gauge, ThreadSafeGauge
from snuba.utils.metrics.timer import Timer
from snuba.utils.threaded_function_delegator import (
    Result as ThreadedFunctionDelegatorResult,
)
from snuba.utils.threaded_function_delegator import ThreadedFunctionDelegator
from snuba.web.query import parse_and_run_query

logger = logging.getLogger("snuba.subscriptions")


class SubscriptionWorker(
    AbstractBatchWorker[Tick, Sequence[SubscriptionTaskResultFuture]]
):
    def __init__(
        self,
        dataset: Dataset,
        executor: ThreadPoolExecutor,
        schedulers: Mapping[int, SubscriptionScheduler],
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
        self, task: ScheduledSubscriptionTask, tick: Tick
    ) -> Tuple[Request, Result]:
        # Measure the amount of time that took between this task being
        # scheduled and it beginning to execute.
        self.__metrics.timing(
            "executor.latency", (time.time() - task.timestamp.timestamp()) * 1000
        )

        # XXX: The ``query`` name is taken from the web views so that all query
        # performance metrics are reported to the same spot, regardless of
        # execution environment.
        timer = Timer(
            "query",
            tags={"partition": str(tick.partition), "entity": task.task.entity.name},
        )

        request = task.task.subscription.data.build_request(
            self.__dataset, task.timestamp, tick.offsets.upper, timer, self.__metrics,
        )
        return self.__execute_query(request, timer, task)

    def __execute_query(
        self, request: Request, timer: Timer, task: ScheduledSubscriptionTask
    ) -> Tuple[Request, Result]:
        with self.__concurrent_gauge:
            is_consistent_query = request.settings.get_consistent()

            def run_consistent() -> Result:
                request_copy = Request(
                    id=request.id,
                    body=copy.deepcopy(request.body),
                    query=copy.deepcopy(request.query),
                    snql_anonymized=request.snql_anonymized,
                    settings=SubscriptionRequestSettings(
                        referrer=request.referrer, consistent=True
                    ),
                )

                return parse_and_run_query(
                    self.__dataset,
                    request_copy,
                    timer,
                    robust=True,
                    concurrent_queries_gauge=self.__concurrent_clickhouse_gauge
                    if is_consistent_query
                    else None,
                ).result

            def run_non_consistent() -> Result:
                request_copy = Request(
                    id=request.id,
                    body=copy.deepcopy(request.body),
                    query=copy.deepcopy(request.query),
                    snql_anonymized=request.snql_anonymized,
                    settings=SubscriptionRequestSettings(
                        referrer=request.referrer, consistent=False
                    ),
                )

                return parse_and_run_query(
                    self.__dataset,
                    request_copy,
                    timer,
                    robust=True,
                    concurrent_queries_gauge=self.__concurrent_clickhouse_gauge
                    if not is_consistent_query
                    else None,
                ).result

            def selector_func(run_snuplicator: bool) -> Tuple[str, List[str]]:
                primary = "consistent" if is_consistent_query else "non_consistent"
                other = "non_consistent" if is_consistent_query else "consistent"

                return (primary, [other] if run_snuplicator else [])

            def callback_func(
                primary_result: Optional[ThreadedFunctionDelegatorResult[Result]],
                other_results: List[ThreadedFunctionDelegatorResult[Result]],
            ) -> None:
                primary_result_data: Optional[Result] = (
                    primary_result.result
                    if primary_result is not None
                    else primary_result
                )
                for result in other_results:
                    match = primary_result_data is not None and handle_differences(
                        result.result
                    ) == handle_differences(primary_result_data)
                    self.__metrics.increment(
                        "consistent",
                        tags={
                            "dataset": self.__dataset_name,
                            "consistent": str(is_consistent_query),
                            "match": str(match),
                        },
                    )

                    if match is False:
                        logger.warning(
                            "Non matching result with consistent and non-consistent subscription query",
                            extra={
                                "dataset": self.__dataset_name,
                                "consistent": str(is_consistent_query),
                                "primary": primary_result_data,
                                "other": result.result,
                                "query_columns": request.query.get_selected_columns(),
                                "query_condition": request.query.get_condition(),
                                "subscription_id": task.task.subscription.identifier,
                                "project": task.task.subscription.data.project_id,
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
            for task in self.__schedulers[message.partition.index].find(tick)
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


def handle_differences(result: Result) -> Result:
    # Handle nan values
    result_copy = copy.deepcopy(result)
    for row in result_copy["data"]:
        for key in row:
            try:
                if math.isnan(row[key]):
                    row[key] = "nan"
            except TypeError:
                pass

    # Strip the profile information as timings will not be the same
    if "profile" in result_copy:
        del result_copy["profile"]

    return result_copy
