from datetime import datetime
from uuid import uuid1

from snuba.datasets.dataset import Dataset
from snuba.redis import redis_client
from snuba.subscriptions.data import Subscription, SubscriptionIdentifier
from snuba.subscriptions.partitioner import DatasetSubscriptionPartitioner
from snuba.subscriptions.store import RedisSubscriptionStore
from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query


class SubscriptionCreator:
    """
    Handles creation of a `Subscription`, including assigning an ID and validating that
    the resulting query is valid.
    """

    def __init__(self, dataset: Dataset):
        self.dataset = dataset

    def create(
        self, subscription: Subscription, timer: Timer
    ) -> SubscriptionIdentifier:
        # We want to test the query out here to make sure it's valid and can run
        request = subscription.build_request(
            self.dataset, datetime.utcnow(), None, timer,
        )
        parse_and_run_query(self.dataset, request, timer)
        partition_id = DatasetSubscriptionPartitioner(self.dataset).build_partition_id(
            subscription
        )
        subscription_id = uuid1().hex
        RedisSubscriptionStore(redis_client, self.dataset, str(partition_id)).create(
            subscription_id, subscription,
        )
        return SubscriptionIdentifier(partition_id, subscription_id)
