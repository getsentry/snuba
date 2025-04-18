import json
import uuid

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.runner import run_job
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.data import PartitionId, RPCSubscriptionData
from snuba.subscriptions.store import RedisSubscriptionDataStore

redis_client = get_redis_client(RedisClientKey.SUBSCRIPTION_STORE)


@pytest.mark.redis_db
def test_rewrite_subscription_entity():
    # Setup test data in Redis
    test_data = {
        "subscription1": {"some": "data"},
        "subscription2": {"other": "data"},
    }

    # Create test data in Redis for spans
    for partition_id in [1, 2]:
        RedisSubscriptionDataStore(
            redis_client, EntityKey("eap_spans"), PartitionId(partition_id)
        ).create(
            uuid.uuid4(),
            RPCSubscriptionData(
                project_id=1,
                resolution_sec=60,
                time_window_sec=300,
                entity=EntityKey("eap_spans"),
                metadata={},
                time_series_request="Ch0IARIJc29tZXRoaW5nGglzb21ldGhpbmciAwECAxIUIhIKBwgBEgNmb28QBhoFEgNiYXIyIQoaCAESDwgDEgt0ZXN0X21ldHJpYxoDc3VtIAEaA3N1bQ==",
                request_name="TimeSeriesRequest",
                request_version="v1",
            ),
        )

    # Create and execute job
    run_job(JobSpec(1, "RewriteSubscriptionEntity"))

    # Verify data was moved correctly
    for partition_id in [1, 2]:
        spans_store = RedisSubscriptionDataStore(
            redis_client, EntityKey("eap_spans"), PartitionId(partition_id)
        )
        items_store = RedisSubscriptionDataStore(
            redis_client, EntityKey("eap_items"), PartitionId(partition_id)
        )

        # Verify data was moved to items
        for sub_id, stored_data in items_store.all():
            assert stored_data is not None
            assert json.loads(stored_data) == test_data

            # Verify data was removed from spans
            assert spans_store.get(sub_id) is None
