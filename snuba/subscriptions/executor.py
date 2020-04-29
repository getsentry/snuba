from __future__ import annotations

import time
from concurrent.futures import Future, ThreadPoolExecutor

from snuba.datasets.dataset import Dataset
from snuba.reader import Result
from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.scheduler import ScheduledTask
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.gauge import Gauge
from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query


class SubscriptionExecutor:
    """
    Responsible for running subscription queries for a given `Dataset`. Each call to
    `execute` should be passed the `ScheduledTask[Subscription]` to run and the `Tick`
    we're running the query for. The `Subscription` generates the `Request` object used
    to make the query, and the result is then returned as a `Future`.
    """

    def __init__(
        self,
        dataset: Dataset,
        executor_pool: ThreadPoolExecutor,
        metrics: MetricsBackend,
    ):
        self.__dataset = dataset
        self.__executor_pool = executor_pool
        self.__metrics = metrics
        self.__concurrent_gauge = Gauge(self.__metrics, "executor.concurrent")

    def __execute(self, task: ScheduledTask[Subscription], tick: Tick) -> Result:
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
            return parse_and_run_query(self.__dataset, request, timer).result

    def execute(self, task: ScheduledTask[Subscription], tick: Tick) -> Future[Result]:
        return self.__executor_pool.submit(self.__execute, task, tick)

    def close(self) -> None:
        self.__executor_pool.shutdown()
