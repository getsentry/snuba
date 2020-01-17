import json
from datetime import timedelta

from snuba.subscriptions.codecs import SubscriptionCodec
from snuba.subscriptions.data import Subscription
from tests.base import BaseTest


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
