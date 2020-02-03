from datetime import datetime
from uuid import UUID, uuid1

from snuba.datasets.dataset import Dataset
from snuba.redis import redis_client
from snuba.subscriptions.data import (
    PartitionId,
    SubscriptionData,
    SubscriptionIdentifier,
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
            uuid1(),
        )
        RedisSubscriptionDataStore(
            redis_client, self.dataset, identifier.partition
        ).create(
            identifier.uuid, data,
        )
        return identifier


class SubscriptionDeleter:
    """
    Handles deletion of a `Subscription`, based on its ID and partition.
    """

    def __init__(self, dataset: Dataset, partition: PartitionId):
        self.dataset = dataset
        self.partition = partition

    def delete(self, subscription_id: UUID) -> None:
        RedisSubscriptionDataStore(redis_client, self.dataset, self.partition).delete(
            subscription_id
        )
