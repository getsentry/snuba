from typing import List, Tuple, cast
from uuid import UUID

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity, get_entity_name
from snuba.datasets.entity_subscriptions.entity_subscription import EntitySubscription
from snuba.datasets.factory import get_dataset
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.data import PartitionId, SubscriptionData
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.subscription import SubscriptionCreator
from snuba.utils.metrics.timer import Timer

dataset = get_dataset("generic_metrics")
entity = get_entity(EntityKey.GENERIC_METRICS_SETS)
entity_key = get_entity_name(entity)
storage = entity.get_writable_storage()
assert storage is not None
stream_loader = storage.get_table_writer().get_stream_loader()
topic_spec = stream_loader.get_default_topic_spec()
org_id = 1

project_id = 1
resolution_sec = 60
time_window_sec = 60
query = "MATCH (generic_metrics_sets) SELECT count() AS count WHERE project_id = 1"

timer = Timer("test_pluggable_entity_subscription")

redis_client = get_redis_client(RedisClientKey.SUBSCRIPTION_STORE)


def subscription_data_builder(
    entity_subscription: EntitySubscription,
) -> SubscriptionData:
    return SubscriptionData(
        project_id=project_id,
        resolution_sec=resolution_sec,
        time_window_sec=time_window_sec,
        entity_subscription=entity_subscription,
        query=query,
    )


@pytest.fixture
def entity_subscriptions() -> EntitySubscription:
    entity_subscription = entity.get_entity_subscription()
    assert isinstance(entity_subscription, EntitySubscription)
    return entity_subscription


def test_entity_subscriptions_data(
    entity_subscriptions: EntitySubscription,
) -> None:
    subscription_data = subscription_data_builder(entity_subscriptions)

    subscription_identifier = SubscriptionCreator(dataset, entity_key).create(
        subscription_data, timer
    )

    stores = [
        RedisSubscriptionDataStore(redis_client, entity_key, PartitionId(i))
        for i in range(topic_spec.partitions_number)
    ]
    print(stores[0].all())
    assert len(stores) == 1
    assert len([s for s in stores[0].all()]) == 1

    result = cast(
        List[Tuple[UUID, SubscriptionData]],
        RedisSubscriptionDataStore(
            redis_client,
            entity_key,
            subscription_identifier.partition,
        ).all(),
    )[0][1]

    assert result.project_id == project_id
    assert result.resolution_sec == resolution_sec
    assert result.time_window_sec == time_window_sec
    assert isinstance(result.entity_subscription, EntitySubscription)
    assert result.query == query
