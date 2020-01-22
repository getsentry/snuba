import json
from datetime import timedelta

from snuba.redis import redis_client
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.store import (
    RedisSubscriptionStore,
    SubscriptionCodec,
)
from tests.base import BaseTest
from tests.subscriptions import BaseSubscriptionTest


class TestRedisSubscriptionStore(BaseSubscriptionTest):
    @property
    def subscription(self) -> Subscription:
        return Subscription(
            project_id=self.project_id,
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        )

    def build_store(self, key="1") -> RedisSubscriptionStore:
        return RedisSubscriptionStore(redis_client, self.dataset, key)

    def test_create(self):
        store = self.build_store()
        subscription_id = "something"
        store.create(subscription_id, self.subscription)
        assert store.all() == [(subscription_id, self.subscription)]

    def test_delete(self):
        store = self.build_store()
        subscription_id = "something"
        store.create(subscription_id, self.subscription)
        assert store.all() == [(subscription_id, self.subscription)]
        store.delete(subscription_id)
        assert store.all() == []

    def test_all(self):
        store = self.build_store()
        assert store.all() == []
        subscription_id = "something"
        store.create(subscription_id, self.subscription)
        assert store.all() == [(subscription_id, self.subscription)]
        new_subscription = Subscription(
            project_id=self.project_id,
            conditions=[["platform", "IN", ["b"]]],
            aggregations=[["count()", "", "something"]],
            time_window=timedelta(minutes=400),
            resolution=timedelta(minutes=2),
        )
        new_subscription_id = "what"
        store.create(new_subscription_id, new_subscription)
        assert sorted(store.all(), key=lambda row: row[0]) == [
            (subscription_id, self.subscription),
            (new_subscription_id, new_subscription),
        ]

    def test_partitions(self):
        store_1 = self.build_store("1")
        store_2 = self.build_store("2")
        subscription_id = "something"
        store_1.create(subscription_id, self.subscription)
        assert store_2.all() == []
        assert store_1.all() == [(subscription_id, self.subscription)]

        new_subscription = Subscription(
            project_id=self.project_id,
            conditions=[["platform", "IN", ["b"]]],
            aggregations=[["count()", "", "something"]],
            time_window=timedelta(minutes=400),
            resolution=timedelta(minutes=2),
        )
        new_subscription_id = "what"
        store_2.create(new_subscription_id, new_subscription)
        assert store_1.all() == [(subscription_id, self.subscription)]
        assert store_2.all() == [(new_subscription_id, new_subscription)]


class TestSubscriptionCodec(BaseTest):
    def build_subscription(self) -> Subscription:
        return Subscription(
            project_id=5,
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        )

    def test_basic(self):
        subscription = self.build_subscription()
        codec = SubscriptionCodec()
        assert codec.decode(codec.encode(subscription)) == subscription

    def test_encode(self):
        codec = SubscriptionCodec()
        subscription = self.build_subscription()

        payload = codec.encode(subscription)
        data = json.loads(payload.decode("utf-8"))
        assert data["project_id"] == subscription.project_id
        assert data["conditions"] == subscription.conditions
        assert data["aggregations"] == subscription.aggregations
        assert data["time_window"] == int(subscription.time_window.total_seconds())
        assert data["resolution"] == int(subscription.resolution.total_seconds())

    def test_decode(self):
        codec = SubscriptionCodec()
        subscription = self.build_subscription()
        data = {
            "project_id": subscription.project_id,
            "conditions": subscription.conditions,
            "aggregations": subscription.aggregations,
            "time_window": int(subscription.time_window.total_seconds()),
            "resolution": int(subscription.resolution.total_seconds()),
        }
        payload = json.dumps(data).encode("utf-8")
        assert codec.decode(payload) == subscription
