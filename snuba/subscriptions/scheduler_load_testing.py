from datetime import datetime, timedelta
from typing import Iterator

from snuba.datasets.entities import EntityKey
from snuba.redis import redis_client
from snuba.subscriptions.data import PartitionId, Subscription
from snuba.subscriptions.scheduler import SubscriptionScheduler
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.utils.metrics import MetricsBackend
from snuba.utils.scheduler import ScheduledTask, Scheduler
from snuba.utils.types import Interval


class LoadTestingSubscriptionScheduler(Scheduler[Subscription]):
    """
    Like SubscriptionScheduler but multiplies the number of subscriptions
    by the load_factor passed. Unlike subscription scheduler this version
    does not take a subscription datastore in the constructor. Rather it
    creates `load_factor` instances of the datastore.

    Used for testing only and should be removed before the scheduler
    is used for any real subscriptions.
    """

    def __init__(
        self,
        partition_id: PartitionId,
        cache_ttl: timedelta,
        metrics: MetricsBackend,
        entity_key: EntityKey,
        load_factor: int,
    ) -> None:
        # Make `load_factor` copies of the scheduler and the store
        self.__scheduler_copies = [
            SubscriptionScheduler(
                RedisSubscriptionDataStore(redis_client, entity_key, partition_id),
                partition_id,
                cache_ttl,
                metrics,
            )
            for _ in range(load_factor)
        ]

    def find(
        self, interval: Interval[datetime]
    ) -> Iterator[ScheduledTask[Subscription]]:
        for scheduler in self.__scheduler_copies:
            for s in scheduler.find(interval):
                yield s
