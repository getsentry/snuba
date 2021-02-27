from datetime import timedelta
from uuid import uuid1

from snuba.redis import redis_client
from snuba.subscriptions.data import SubscriptionData
from snuba.subscriptions.store import RedisSubscriptionDataStore
from tests.subscriptions import BaseSubscriptionTest


class TestRedisSubscriptionStore(BaseSubscriptionTest):
    @property
    def subscription(self) -> SubscriptionData:
        return SubscriptionData(
            project_id=self.project_id,
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        )

    def build_store(self, key="1") -> RedisSubscriptionDataStore:
        return RedisSubscriptionDataStore(redis_client, self.dataset, key)

    def test_create(self) -> None:
        store = self.build_store()
        subscription_id = uuid1()
        store.create(subscription_id, self.subscription)
        assert store.all() == [(subscription_id, self.subscription)]

    def test_delete(self) -> None:
        store = self.build_store()
        subscription_id = uuid1()
        store.create(subscription_id, self.subscription)
        assert store.all() == [(subscription_id, self.subscription)]
        store.delete(subscription_id)
        assert store.all() == []

    def test_all(self) -> None:
        store = self.build_store()
        assert store.all() == []
        subscription_id = uuid1()
        store.create(subscription_id, self.subscription)
        assert store.all() == [(subscription_id, self.subscription)]
        new_subscription = SubscriptionData(
            project_id=self.project_id,
            conditions=[["platform", "IN", ["b"]]],
            aggregations=[["count()", "", "something"]],
            time_window=timedelta(minutes=400),
            resolution=timedelta(minutes=2),
        )
        new_subscription_id = uuid1()
        store.create(new_subscription_id, new_subscription)
        assert sorted(store.all(), key=lambda row: row[0]) == [
            (subscription_id, self.subscription),
            (new_subscription_id, new_subscription),
        ]

    def test_partitions(self) -> None:
        store_1 = self.build_store("1")
        store_2 = self.build_store("2")
        subscription_id = uuid1()
        store_1.create(subscription_id, self.subscription)
        assert store_2.all() == []
        assert store_1.all() == [(subscription_id, self.subscription)]

        new_subscription = SubscriptionData(
            project_id=self.project_id,
            conditions=[["platform", "IN", ["b"]]],
            aggregations=[["count()", "", "something"]],
            time_window=timedelta(minutes=400),
            resolution=timedelta(minutes=2),
        )
        new_subscription_id = uuid1()
        store_2.create(new_subscription_id, new_subscription)
        assert store_1.all() == [(subscription_id, self.subscription)]
        assert store_2.all() == [(new_subscription_id, new_subscription)]
