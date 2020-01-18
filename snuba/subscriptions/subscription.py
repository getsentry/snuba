from datetime import datetime
from uuid import uuid1

from snuba.datasets.dataset import Dataset
from snuba.redis import redis_client
from snuba.subscriptions.data import (
    SubscriptionData,
    SubscriptionIdentifier,
    SubscriptionKey,
)
from snuba.subscriptions.partitioner import DatasetSubscriptionDataPartitioner
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query


class SubscriptionCreator:
    """
    Handles creation of a `Subscription`, including assigning an ID and validating that
    the resulting query is valid.
    """

    def __init__(self, dataset: Dataset):
        self.dataset = dataset

    def create(self, data: SubscriptionData, timer: Timer) -> SubscriptionIdentifier:
        # We want to test the query out here to make sure it's valid and can run
        request = data.build_request(self.dataset, datetime.utcnow(), None, timer)
        parse_and_run_query(self.dataset, request, timer)
        identifier = SubscriptionIdentifier(
            DatasetSubscriptionDataPartitioner(self.dataset).build_partition_id(data),
            SubscriptionKey(uuid1().hex),
        )
        RedisSubscriptionDataStore(
            redis_client, self.dataset, identifier.partition
        ).create(
            identifier.key, data,
        )
        return identifier
