from typing import List, Optional, Tuple, cast
from uuid import UUID

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity, get_entity_name
from snuba.datasets.entity_subscriptions.entity_subscription import (
    EntitySubscription,
    GenericMetricsSetsSubscription,
)
from snuba.datasets.entity_subscriptions.pluggable_entity_subscription import (
    PluggableEntitySubscription,
)
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
data = {"organization": 1}

timer = Timer("test_pluggable_entity_subscription")

redis_client = get_redis_client(RedisClientKey.SUBSCRIPTION_STORE)


def subscription_data_builder(
    entity_subscription: EntitySubscription, organization: Optional[int] = None
) -> SubscriptionData:
    return SubscriptionData(
        project_id=1,
        resolution_sec=60,
        time_window_sec=60,
        entity_subscription=entity_subscription,
        query="MATCH (generic_metrics_sets) SELECT count() AS count WHERE project_id = 1",
        organization=organization,
    )


@pytest.fixture
def pluggable_sets_entity_subscription() -> EntitySubscription:
    PluggableEntitySubscription.name = "generic_metrics_sets_subscription"
    PluggableEntitySubscription.MAX_ALLOWED_AGGREGATIONS = 3
    PluggableEntitySubscription.disallowed_aggregations = ["having", "orderby"]
    assert issubclass(PluggableEntitySubscription, EntitySubscription)
    return PluggableEntitySubscription()


def test_entity_subscription_generic_metrics_sets_regular_vs_pluggable(
    pluggable_sets_entity_subscription: PluggableEntitySubscription,
) -> None:
    sets_entity_subscription = GenericMetricsSetsSubscription()

    sets_subscription = subscription_data_builder(sets_entity_subscription, 1)
    assert isinstance(pluggable_sets_entity_subscription, EntitySubscription)
    pluggable_subscription = subscription_data_builder(
        pluggable_sets_entity_subscription, 1
    )

    sets_identifier = SubscriptionCreator(dataset, entity_key).create(
        sets_subscription, timer
    )
    pluggable_identifier = SubscriptionCreator(dataset, entity_key).create(
        pluggable_subscription, timer
    )

    stores = [
        RedisSubscriptionDataStore(redis_client, entity_key, PartitionId(i))
        for i in range(topic_spec.partitions_number)
    ]

    assert len(stores) == 1
    assert len([s for s in stores[0].all()]) == 2

    sets_result = cast(
        List[Tuple[UUID, SubscriptionData]],
        RedisSubscriptionDataStore(
            redis_client,
            entity_key,
            sets_identifier.partition,
        ).all(),
    )[0][1]
    pluggable_results = cast(
        List[Tuple[UUID, SubscriptionData]],
        RedisSubscriptionDataStore(
            redis_client,
            entity_key,
            pluggable_identifier.partition,
        ).all(),
    )[0][1]

    assert sets_result.project_id == pluggable_results.project_id
    assert sets_result.resolution_sec == pluggable_results.resolution_sec
    assert sets_result.time_window_sec == pluggable_results.time_window_sec
    assert (
        sets_result.entity_subscription.__class__
        == pluggable_results.entity_subscription.__class__
    )
    assert sets_result.query == pluggable_results.query
