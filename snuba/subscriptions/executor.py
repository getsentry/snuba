from __future__ import annotations

import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime

from snuba.datasets.dataset import Dataset
from snuba.reader import Result
from snuba.request import Request
from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.scheduler import ScheduledTask
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.gauge import Gauge
from snuba.utils.metrics.timer import Timer
from snuba.web.query import run_query


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

    def __run_query(
        self, scheduled_at: datetime, request: Request, timer: Timer
    ) -> Result:
        self.__metrics.timing(
            "executor.latency", (time.time() - scheduled_at.timestamp()) * 1000,
        )
        with self.__concurrent_gauge:
            return run_query(self.__dataset, request, timer).result

    def execute(self, task: ScheduledTask[Subscription], tick: Tick) -> Future[Result]:
        timer = Timer("query")
        try:
            request = task.task.data.build_request(
                self.__dataset, task.timestamp, tick.offsets.upper, timer,
            )
        except Exception as e:
            future: Future[Result] = Future()
            future.set_exception(e)
        else:
            future = self.__executor_pool.submit(
                self.__run_query, task.timestamp, request, timer
            )
        return future

    def close(self) -> None:
        self.__executor_pool.shutdown()
