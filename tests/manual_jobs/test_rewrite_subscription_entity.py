import uuid

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.runner import run_job
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.data import PartitionId, RPCSubscriptionData
from snuba.subscriptions.store import RedisSubscriptionDataStore

redis_client = get_redis_client(RedisClientKey.SUBSCRIPTION_STORE)


@pytest.mark.redis_db
def test_rewrite_subscription_entity() -> None:
    request_contents = "Ch0IARIJc29tZXRoaW5nGglzb21ldGhpbmciAwECAxIUIhIKBwgBEgNmb28QBhoFEgNiYXIyIQoaCAESDwgDEgt0ZXN0X21ldHJpYxoDc3VtIAEaA3N1bQ=="

    # Create eap_spans subscriptions
    for partition_id in [1, 2]:
        RedisSubscriptionDataStore(
            redis_client, EntityKey("eap_spans"), PartitionId(partition_id)
        ).create(
            uuid.uuid4(),
            RPCSubscriptionData(
                project_id=1,
                resolution_sec=60,
                time_window_sec=300,
                entity=get_entity(EntityKey.EAP_SPANS),
                metadata={},
                time_series_request=request_contents,
                request_name="TimeSeriesRequest",
                request_version="v1",
            ),
        )

    # Execute job
    run_job(
        JobSpec(
            "jobid",
            "RewriteSubscriptionEntity",
            False,
            {"source_entity": "eap_spans", "target_entity": "eap_items"},
        )
    )

    # Verify data was moved correctly
    for partition_id in [1, 2]:
        spans_store = RedisSubscriptionDataStore(
            redis_client, EntityKey("eap_spans"), PartitionId(partition_id)
        )
        items_store = RedisSubscriptionDataStore(
            redis_client, EntityKey("eap_items"), PartitionId(partition_id)
        )

        # Verify data was moved to items
        for _, stored_data in items_store.all():
            assert stored_data is not None
            assert isinstance(stored_data, RPCSubscriptionData)
            assert stored_data.entity == get_entity(EntityKey.EAP_ITEMS)
            assert stored_data.time_series_request == request_contents

            # Verify data was removed from spans
            assert spans_store.all() == []
