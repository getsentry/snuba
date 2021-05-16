from datetime import timedelta
from typing import Sequence
from uuid import uuid1

from snuba.redis import redis_client
from snuba.subscriptions.data import (
    LegacySubscriptionData,
    PartitionId,
    SnQLSubscriptionData,
    SubscriptionData,
)
from snuba.subscriptions.store import RedisSubscriptionDataStore
from tests.subscriptions import BaseSubscriptionTest


class TestRedisSubscriptionStore(BaseSubscriptionTest):
    @property
    def subscription(self) -> Sequence[SubscriptionData]:
        return [
            LegacySubscriptionData(
                project_id=self.project_id,
                conditions=[["platform", "IN", ["a"]]],
                aggregations=[["count()", "", "count"]],
                time_window=timedelta(minutes=500),
                resolution=timedelta(minutes=1),
            ),
            SnQLSubscriptionData(
                project_id=self.project_id,
                time_window=timedelta(minutes=500),
                resolution=timedelta(minutes=1),
                query="MATCH events SELECT count() WHERE in(platform, 'a')",
            ),
        ]

    def build_store(self, key: int = 1) -> RedisSubscriptionDataStore:
        return RedisSubscriptionDataStore(redis_client, self.dataset, PartitionId(key))

    def test_create(self) -> None:
        store = self.build_store()
        subscription_id_1 = uuid1()
        store.create(subscription_id_1, self.subscription[0])
        subscription_id_2 = uuid1()
        store.create(subscription_id_2, self.subscription[1])
        assert sorted(store.all()) == [
            (subscription_id_1, self.subscription[0]),
            (subscription_id_2, self.subscription[1]),
        ]

    def test_delete(self) -> None:
        store = self.build_store()
        subscription_id = uuid1()
        store.create(subscription_id, self.subscription[0])
        assert store.all() == [(subscription_id, self.subscription[0])]
        store.delete(subscription_id)
        assert store.all() == []

    def test_all(self) -> None:
        store = self.build_store()
        assert store.all() == []
        subscription_id = uuid1()
        store.create(subscription_id, self.subscription[0])
        assert store.all() == [(subscription_id, self.subscription[0])]
        new_subscription_id = uuid1()
        store.create(new_subscription_id, self.subscription[1])
        assert sorted(store.all(), key=lambda row: row[0]) == [
            (subscription_id, self.subscription[0]),
            (new_subscription_id, self.subscription[1]),
        ]

    def test_partitions(self) -> None:
        store_1 = self.build_store(1)
        store_2 = self.build_store(2)
        subscription_id = uuid1()
        store_1.create(subscription_id, self.subscription[0])
        assert store_2.all() == []
        assert store_1.all() == [(subscription_id, self.subscription[0])]

        new_subscription_id = uuid1()
        store_2.create(new_subscription_id, self.subscription[1])
        assert store_1.all() == [(subscription_id, self.subscription[0])]
        assert store_2.all() == [(new_subscription_id, self.subscription[1])]
