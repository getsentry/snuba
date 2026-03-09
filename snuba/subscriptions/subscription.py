from datetime import datetime
from uuid import UUID, uuid1

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import enforce_table_writer, get_entity
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.data import (
    PartitionId,
    SubscriptionData,
    SubscriptionIdentifier,
)
from snuba.subscriptions.partitioner import TopicSubscriptionDataPartitioner
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.utils.metrics.timer import Timer

redis_client = get_redis_client(RedisClientKey.SUBSCRIPTION_STORE)


class SubscriptionCreator:
    """
    Handles creation of a `Subscription`, including assigning an ID and validating that
    the resulting query is valid.
    """

    def __init__(self, dataset: Dataset, entity_key: EntityKey):
        self.dataset = dataset
        self.entity_key = entity_key

        entity = get_entity(entity_key)
        self.__partitioner = TopicSubscriptionDataPartitioner(
            enforce_table_writer(entity).get_stream_loader().get_default_topic_spec()
        )

    def create(self, data: SubscriptionData, timer: Timer) -> SubscriptionIdentifier:
        data.validate()

        self._test_request(data, timer)

        identifier = SubscriptionIdentifier(
            self.__partitioner.build_partition_id(data),
            uuid1(),
        )
        RedisSubscriptionDataStore(redis_client, self.entity_key, identifier.partition).create(
            identifier.uuid,
            data,
        )
        return identifier

    def _test_request(self, data: SubscriptionData, timer: Timer) -> None:
        request = data.build_request(self.dataset, datetime.utcnow(), None, timer)
        data.run_query(self.dataset, request, timer)  # type: ignore


class SubscriptionDeleter:
    """
    Handles deletion of a `Subscription`, based on its ID and partition.
    """

    def __init__(self, entity_key: EntityKey, partition: PartitionId):
        self.entity_key = entity_key
        self.partition = partition

    def delete(self, subscription_id: UUID) -> None:
        RedisSubscriptionDataStore(redis_client, self.entity_key, self.partition).delete(
            subscription_id
        )
