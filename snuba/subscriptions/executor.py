from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor

from snuba.api.query import ClickhouseQueryResult, parse_and_run_query
from snuba.datasets.dataset import Dataset
from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.scheduler import ScheduledTask
from snuba.utils.metrics.timer import Timer


class SubscriptionExecutor:
    """
    Responsible for running subscription queries for a given `Dataset`. Each call to
    `execute` should be passed the `ScheduledTask[Subscription]` to run and the `Tick`
    we're running the query for. The `Subscription` generates the `Request` object used
    to make the query, and the result is then returned as a `Future`.
    """

    def __init__(self, dataset: Dataset, executor_pool: ThreadPoolExecutor):
        self.__dataset = dataset
        self.__executor_pool = executor_pool

    def execute(
        self, task: ScheduledTask[Subscription], tick: Tick, timer: Timer
    ) -> Future[ClickhouseQueryResult]:
        try:
            request = task.task.build_request(
                self.__dataset, tick.timestamps.upper, tick.offsets.upper, timer,
            )
        except Exception as e:
            future: Future[ClickhouseQueryResult] = Future()
            future.set_exception(e)
        else:
            future = self.__executor_pool.submit(
                parse_and_run_query, self.__dataset, request, timer
            )
        return future

    def close(self) -> None:
        self.__executor_pool.shutdown()
