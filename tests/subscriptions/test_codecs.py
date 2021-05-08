import json
import uuid
from datetime import datetime, timedelta

from snuba.datasets.factory import get_dataset
from snuba.reader import Result
from snuba.subscriptions.codecs import (
    SubscriptionDataCodec,
    SubscriptionTaskResultEncoder,
)
from snuba.subscriptions.data import (
    LegacySubscriptionData,
    PartitionId,
    Subscription,
    SubscriptionIdentifier,
)
from snuba.subscriptions.worker import SubscriptionTaskResult
from snuba.utils.metrics.timer import Timer
from snuba.utils.scheduler import ScheduledTask


class TestSubscriptionCodec:
    def build_subscription_data(self) -> LegacySubscriptionData:
        return LegacySubscriptionData(
            project_id=5,
            conditions=[["platform", "IN", ["a"]]],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        )

    def test_basic(self) -> None:
        data = self.build_subscription_data()
        codec = SubscriptionDataCodec()
        assert codec.decode(codec.encode(data)) == data

    def test_encode(self) -> None:
        codec = SubscriptionDataCodec()
        subscription = self.build_subscription_data()

        payload = codec.encode(subscription)
        data = json.loads(payload.decode("utf-8"))
        assert data["project_id"] == subscription.project_id
        assert data["conditions"] == subscription.conditions
        assert data["aggregations"] == subscription.aggregations
        assert data["time_window"] == int(subscription.time_window.total_seconds())
        assert data["resolution"] == int(subscription.resolution.total_seconds())

    def test_decode(self) -> None:
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


def test_subscription_task_result_encoder() -> None:
    codec = SubscriptionTaskResultEncoder()

    timestamp = datetime.now()

    subscription_data = LegacySubscriptionData(
        project_id=1,
        conditions=[],
        aggregations=[["count()", "", "count"]],
        time_window=timedelta(minutes=1),
        resolution=timedelta(minutes=1),
    )

    # XXX: This seems way too coupled to the dataset.
    request = subscription_data.build_request(
        get_dataset("events"), timestamp, None, Timer("timer")
    )
    result: Result = {
        "meta": [{"type": "UInt64", "name": "count"}],
        "data": [{"count": 1}],
    }

    task_result = SubscriptionTaskResult(
        ScheduledTask(
            timestamp,
            Subscription(
                SubscriptionIdentifier(PartitionId(1), uuid.uuid1()), subscription_data,
            ),
        ),
        (request, result),
    )

    message = codec.encode(task_result)
    data = json.loads(message.value.decode("utf-8"))
    assert data["version"] == 2
    payload = data["payload"]

    assert payload["subscription_id"] == str(task_result.task.task.identifier)
    assert payload["request"] == request.body
    assert payload["result"] == result
    assert payload["timestamp"] == task_result.task.timestamp.isoformat()
