from typing import List, Tuple, cast
from uuid import UUID

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity, get_entity_name
from snuba.datasets.factory import get_dataset
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.data import PartitionId, SubscriptionData
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.subscription import SubscriptionCreator
from snuba.utils.metrics.timer import Timer

dataset = get_dataset("generic_metrics")
entity = get_entity(EntityKey.GENERIC_METRICS_SETS)
entity_key = get_entity_name(entity)
storage = get_storage(StorageKey.GENERIC_METRICS_SETS_RAW)
assert storage is not None
assert isinstance(storage, WritableTableStorage)
stream_loader = storage.get_table_writer().get_stream_loader()
topic_spec = stream_loader.get_default_topic_spec()
org_id = 1
metadata = {"organization": org_id}

project_id = 1
resolution_sec = 60
time_window_sec = 60
query = "MATCH (generic_metrics_sets) SELECT count() AS count WHERE project_id = 1"

timer = Timer("test_entity_subscription_data")

redis_client = get_redis_client(RedisClientKey.SUBSCRIPTION_STORE)


def subscription_data_builder() -> SubscriptionData:
    return SubscriptionData(
        project_id=project_id,
        resolution_sec=resolution_sec,
        time_window_sec=time_window_sec,
        entity=entity,
        query=query,
        metadata=metadata,
    )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_entity_subscriptions_data() -> None:
    subscription_data = subscription_data_builder()

    subscription_identifier = SubscriptionCreator(dataset, entity_key).create(
        subscription_data, timer
    )

    stores = [
        RedisSubscriptionDataStore(redis_client, entity_key, PartitionId(i))
        for i in range(topic_spec.partitions_number)
    ]

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
    assert result.entity == entity
    assert result.query == query
