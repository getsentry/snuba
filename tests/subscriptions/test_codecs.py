import json
import uuid
from datetime import datetime, timedelta

from snuba.subscriptions.codecs import SubscriptionDataCodec
from snuba.subscriptions.data import (
    PartitionId,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
)
from snuba.subscriptions.worker import SubscriptionResult, SubscriptionResultCodec
from snuba.subscriptions.scheduler import ScheduledTask
from tests.base import BaseTest


class TestSubscriptionCodec(BaseTest):
    def build_subscription_data(self) -> SubscriptionData:
        return SubscriptionData(
            project_id=5,
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        )

    def test_basic(self):
        data = self.build_subscription_data()
        codec = SubscriptionDataCodec()
        assert codec.decode(codec.encode(data)) == data

    def test_encode(self):
        codec = SubscriptionDataCodec()
        subscription = self.build_subscription_data()

        payload = codec.encode(subscription)
        data = json.loads(payload.decode("utf-8"))
        assert data["project_id"] == subscription.project_id
        assert data["conditions"] == subscription.conditions
        assert data["aggregations"] == subscription.aggregations
        assert data["time_window"] == int(subscription.time_window.total_seconds())
        assert data["resolution"] == int(subscription.resolution.total_seconds())

    def test_decode(self):
        codec = SubscriptionDataCodec()
        subscription = self.build_subscription_data()
        data = {
            "project_id": subscription.project_id,
            "conditions": subscription.conditions,
            "aggregations": subscription.aggregations,
            "time_window": int(subscription.time_window.total_seconds()),
            "resolution": int(subscription.resolution.total_seconds()),
        }
        payload = json.dumps(data).encode("utf-8")
        assert codec.decode(payload) == subscription


class TestQueryResultCodec(BaseTest):
    def test_encode(self):
        result = SubscriptionResult(
            ScheduledTask(
                datetime.now(),
                Subscription(
                    SubscriptionIdentifier(PartitionId(1), uuid.uuid1()),
                    SubscriptionData(
                        1,
                        [],
                        [["count()", "", "count"]],
                        timedelta(minutes=1),
                        timedelta(minutes=1),
                    ),
                ),
            ),
            {"data": {"count": 100}},
        )

        codec = SubscriptionResultCodec()
        message = codec.encode(result)
        data = json.loads(message.value.decode("utf-8"))
        assert data["version"] == 1
        payload = data["payload"]

        assert payload["subscription_id"] == str(result.task.task.identifier)
        assert payload["values"] == result.result["data"]
        assert payload["timestamp"] == result.task.timestamp.timestamp()
