import uuid
from datetime import datetime, timedelta

from snuba.datasets.entities import EntityKey
from snuba.redis import redis_client
from snuba.subscriptions.data import PartitionId, SnQLSubscriptionData
from snuba.subscriptions.entity_subscription import EventsSubscription
from snuba.subscriptions.scheduler_load_testing import LoadTestingSubscriptionScheduler
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.utils import Tick
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.types import Interval


def test_scheduler_load_testing() -> None:
    entity_key = EntityKey.EVENTS
    partition_id = PartitionId(0)
    metrics = DummyMetricsBackend()
    load_factor = 5

    store = RedisSubscriptionDataStore(redis_client, entity_key, partition_id)
    store.create(
        uuid.uuid4(),
        SnQLSubscriptionData(
            project_id=1,
            time_window=timedelta(minutes=1),
            resolution=timedelta(minutes=1),
            query="MATCH events SELECT count()",
            entity_subscription=EventsSubscription(data_dict={}),
        ),
    )

    scheduler = LoadTestingSubscriptionScheduler(
        partition_id, timedelta(seconds=300), metrics, entity_key, load_factor
    )
    now = datetime.now()
    tick = Tick(0, Interval(1, 2), Interval(now - timedelta(minutes=1), now))
    assert len([s for s in scheduler.find(tick)]) == load_factor
