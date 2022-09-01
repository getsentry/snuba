from typing import List, Tuple, cast
from uuid import UUID

import pytest

from snuba.datasets.entities.factory import get_entity_name
from snuba.datasets.factory import get_dataset
from snuba.redis import redis_client
from snuba.subscriptions.data import PartitionId, SubscriptionData
from snuba.subscriptions.entity_subscriptions.entity_subscription import (
    EntitySubscription,
    GenericMetricsSetsSubscription,
    SessionsSubscription,
)
from snuba.subscriptions.entity_subscriptions.pluggable_entity_subscription import (
    PluggableEntitySubscription,
)
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.subscription import SubscriptionCreator
from snuba.utils.metrics.timer import Timer

dataset = get_dataset("generic_metrics")
entity = dataset.get_default_entity()
entity_key = get_entity_name(entity)
storage = entity.get_writable_storage()
assert storage is not None
stream_loader = storage.get_table_writer().get_stream_loader()
topic_spec = stream_loader.get_default_topic_spec()
data = {"organization": 1}

timer = Timer("test_pluggable_entity_subscription")


def subscription_data_builder(
    entity_subscription: EntitySubscription,
) -> SubscriptionData:
    return SubscriptionData(
        project_id=1,
        resolution_sec=60,
        time_window_sec=60,
        entity_subscription=entity_subscription,
        query="MATCH (generic_metrics_sets) SELECT count() AS count WHERE project_id = 1",
    )


@pytest.fixture
def pluggable_sets_entity_subscription() -> EntitySubscription:
    PluggableEntitySubscription.name = "generic_metrics_sets_subscription"
    PluggableEntitySubscription.MAX_ALLOWED_AGGREGATIONS = 3
    PluggableEntitySubscription.disallowed_aggregations = ["having", "orderby"]
    NewPluggableEntitySubscription = type(
        "PluggableEntitySubscription",
        (SessionsSubscription,),
        PluggableEntitySubscription.__dict__.copy(),
    )
    assert issubclass(NewPluggableEntitySubscription, EntitySubscription)
    return NewPluggableEntitySubscription(data_dict=data)


def test_entity_subscription_generic_metrics_sets_regular_vs_pluggable(
    pluggable_sets_entity_subscription: PluggableEntitySubscription,
) -> None:
    sets_entity_subscription = GenericMetricsSetsSubscription(data_dict=data)

    sets_subscription = subscription_data_builder(sets_entity_subscription)
    assert isinstance(pluggable_sets_entity_subscription, EntitySubscription)
    pluggable_subscription = subscription_data_builder(
        pluggable_sets_entity_subscription
    )

    sets_identifier = SubscriptionCreator(dataset, entity_key).create(
        sets_subscription, timer
    )
    print(f"Created {sets_identifier}")

    pluggable_identifier = SubscriptionCreator(dataset, entity_key).create(
        pluggable_subscription, timer
    )
    print(f"Created {pluggable_identifier}")

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
