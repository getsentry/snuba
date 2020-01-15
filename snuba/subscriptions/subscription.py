from datetime import datetime, timedelta
from typing import Sequence, Tuple
from uuid import uuid1

from snuba.datasets.dataset import Dataset
from snuba.query.query import Aggregation
from snuba.query.types import Condition
from snuba.redis import redis_client
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.partitioner import DatasetSubscriptionPartitioner
from snuba.subscriptions.store import RedisSubscriptionStore
from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query


class InvalidSubscriptionError(Exception):
    pass


class SubscriptionCreator:
    """
    Handles creation of a `Subscription`, including assigning an ID and validating that
    the resulting query is valid.
    """

    def __init__(self, dataset: Dataset):
        self.dataset = dataset

    def create(
        self,
        project_id: int,
        conditions: Sequence[Condition],
        aggregations: Sequence[Aggregation],
        time_window: timedelta,
        resolution: timedelta,
        timer: Timer,
    ) -> Tuple[str, str]:
        if time_window < timedelta(minutes=1):
            raise InvalidSubscriptionError(
                "Time window must be greater than or equal to 1 minute"
            )
        if resolution < timedelta(minutes=1):
            raise InvalidSubscriptionError(
                "Resolution must be greater than or equal to 1 minute"
            )

        subscription = Subscription(
            str(uuid1().hex),
            project_id,
            conditions,
            aggregations,
            time_window,
            resolution,
        )

        # We want to test the query out here to make sure it's valid and can run
        request = subscription.build_request(
            self.dataset, datetime.now() - subscription.time_window, None, timer,
        )
        parse_and_run_query(self.dataset, request, timer)
        partition_id = DatasetSubscriptionPartitioner(self.dataset).build_partition_id(
            subscription
        )
        RedisSubscriptionStore(redis_client, partition_id).create(subscription)
        return partition_id, subscription
