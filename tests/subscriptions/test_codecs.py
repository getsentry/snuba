import json
import uuid
from datetime import datetime, timedelta

from snuba.subscriptions.codecs import (
    QueryResult,
    QueryResultCodec,
    SubscriptionDataCodec,
)
from snuba.subscriptions.data import (
    PartitionId,
    SubscriptionData,
    SubscriptionIdentifier,
)
from snuba.utils.streams.kafka import KafkaPayload
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
    def build_data(self) -> QueryResult:
        partition_id = PartitionId(1)
        return QueryResult(
            SubscriptionIdentifier(partition_id, uuid.uuid4()),
            datetime.now(),
            {"count": 100},
            partition_id,
            2,
            timedelta(minutes=10),
        )

    def test_basic(self):
        data = self.build_data()
        codec = QueryResultCodec()
        assert codec.decode(codec.encode(data)) == data

    def test_encode(self):
        codec = QueryResultCodec()
        test_data = self.build_data()

        message = codec.encode(test_data)
        data = json.loads(message.value.decode("utf-8"))
        assert data["version"] == 1
        payload = data["payload"]

        assert payload["subscription_id"] == str(test_data.subscription_id)
        assert payload["values"] == test_data.values
        assert payload["timestamp"] == test_data.timestamp.timestamp()
        assert payload["interval"] == test_data.interval.total_seconds()
        assert payload["partition"] == test_data.partition
        assert payload["offset"] == test_data.offset

    def test_decode(self):
        codec = QueryResultCodec()
        test_data = self.build_data()
        data = {
            "version": 1,
            "payload": {
                "subscription_id": str(test_data.subscription_id),
                "values": test_data.values,
                "timestamp": test_data.timestamp.timestamp(),
                "interval": int(test_data.interval.total_seconds()),
                "partition": test_data.partition,
                "offset": test_data.offset,
            },
        }
        payload = json.dumps(data).encode("utf-8")
        message = KafkaPayload(str(test_data.subscription_id).encode("utf-8"), payload)
        assert codec.decode(message) == test_data
