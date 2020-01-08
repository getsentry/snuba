from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor

from snuba import settings
from snuba.api.query import ClickhouseQueryResult, parse_and_run_query
from snuba.datasets.dataset import Dataset
from snuba.request import Request
from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.scheduler import ScheduledTask
from snuba.utils.metrics.timer import Timer


executor_pool = ThreadPoolExecutor(max_workers=settings.SUBSCRIPTIONS_MAX_CONCURRENT_QUERIES)


class Executor:
    """
    Responsible for running subscription queries for a given `Dataset`. Each call to
    `execute` should be passed the `ScheduledTask[Subscription]` to run and the `Tick`
    we're running the query for. The `Subscription` generates the `Request` object used
    to make the query, and the result is then returned as a `Future`.
    """

    def __init__(self, dataset: Dataset):
        self.dataset = dataset

    def execute(
        self, task: ScheduledTask[Subscription], tick: Tick, timer: Timer
    ) -> Future[ClickhouseQueryResult]:
        request: Request = task.task.build_request(
            self.dataset,
            tick.timestamps.upper,
            tick.offsets.upper,
            timer,
        )
        return executor_pool.submit(parse_and_run_query, self.dataset, request, timer)
