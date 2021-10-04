from datetime import datetime
from uuid import UUID, uuid1

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import ENTITY_NAME_LOOKUP
from snuba.datasets.factory import enforce_table_writer
from snuba.redis import redis_client
from snuba.subscriptions.data import (
    DelegateSubscriptionData,
    PartitionId,
    SubscriptionData,
    SubscriptionIdentifier,
)
from snuba.subscriptions.partitioner import TopicSubscriptionDataPartitioner
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
        self.entity_key = ENTITY_NAME_LOOKUP[dataset.get_default_entity()]

        self.__partitioner = TopicSubscriptionDataPartitioner(
            enforce_table_writer(dataset).get_stream_loader().get_default_topic_spec()
        )

    def create(self, data: SubscriptionData, timer: Timer) -> SubscriptionIdentifier:
        # We want to test the query out here to make sure it's valid and can run
        # If there is a delegate subscription, we need to run both the SnQL and Legacy validator
        if isinstance(data, DelegateSubscriptionData):
            self._test_request(data.to_snql(), timer)
            self._test_request(data.to_legacy(), timer)
        else:
            self._test_request(data, timer)

        identifier = SubscriptionIdentifier(
            self.__partitioner.build_partition_id(data), uuid1(),
        )
        RedisSubscriptionDataStore(
            redis_client, self.entity_key, identifier.partition
        ).create(
            identifier.uuid, data,
        )
        return identifier

    def _test_request(self, data: SubscriptionData, timer: Timer) -> None:
        request = data.build_request(self.dataset, datetime.utcnow(), None, timer)
        parse_and_run_query(self.dataset, request, timer)


class SubscriptionDeleter:
    """
    Handles deletion of a `Subscription`, based on its ID and partition.
    """

    def __init__(self, dataset: Dataset, partition: PartitionId):
        self.dataset = dataset
        self.entity_key = ENTITY_NAME_LOOKUP[dataset.get_default_entity()]
        self.partition = partition

    def delete(self, subscription_id: UUID) -> None:
        RedisSubscriptionDataStore(
            redis_client, self.entity_key, self.partition
        ).delete(subscription_id)
