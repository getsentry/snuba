from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from typing import Callable

import pytest

from snuba.datasets.factory import get_dataset
from snuba.reader import Result
from snuba.subscriptions.codecs import (
    SubscriptionDataCodec,
    SubscriptionTaskResultEncoder,
)
from snuba.subscriptions.data import (
    DelegateSubscriptionData,
    LegacySubscriptionData,
    PartitionId,
    SnQLSubscriptionData,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
    SubscriptionType,
)
from snuba.subscriptions.worker import SubscriptionTaskResult
from snuba.utils.metrics.timer import Timer
from snuba.utils.scheduler import ScheduledTask


def build_legacy_subscription_data() -> LegacySubscriptionData:
    return LegacySubscriptionData(
        project_id=5,
        conditions=[["platform", "IN", ["a"]]],
        aggregations=[["count()", "", "count"]],
        time_window=timedelta(minutes=500),
        resolution=timedelta(minutes=1),
    )


def build_snql_subscription_data() -> SnQLSubscriptionData:
    return SnQLSubscriptionData(
        project_id=5,
        time_window=timedelta(minutes=500),
        resolution=timedelta(minutes=1),
        query="MATCH events SELECT count() WHERE in(platform, 'a')",
    )


def build_delegate_subscription_data() -> DelegateSubscriptionData:
    return DelegateSubscriptionData(
        project_id=5,
        time_window=timedelta(minutes=500),
        resolution=timedelta(minutes=1),
        conditions=[["platform", "IN", ["a"]]],
        aggregations=[["count()", "", "count"]],
        query="MATCH events SELECT count() WHERE in(platform, 'a')",
    )


BASIC_CASES = [
    pytest.param(build_legacy_subscription_data, id="legacy"),
    pytest.param(build_snql_subscription_data, id="snql",),
    pytest.param(build_delegate_subscription_data, id="delegate"),
]


@pytest.mark.parametrize("builder", BASIC_CASES)
def test_basic(builder: Callable[[], SubscriptionData]) -> None:
    codec = SubscriptionDataCodec()
    data = builder()
    assert codec.decode(codec.encode(data)) == data


def test_encode() -> None:
    codec = SubscriptionDataCodec()
    subscription = build_legacy_subscription_data()

    payload = codec.encode(subscription)
    data = json.loads(payload.decode("utf-8"))
    assert data["project_id"] == subscription.project_id
    assert data["conditions"] == subscription.conditions
    assert data["aggregations"] == subscription.aggregations
    assert data["time_window"] == int(subscription.time_window.total_seconds())
    assert data["resolution"] == int(subscription.resolution.total_seconds())


def test_encode_snql() -> None:
    codec = SubscriptionDataCodec()
    subscription = build_snql_subscription_data()

    payload = codec.encode(subscription)
    data = json.loads(payload.decode("utf-8"))
    assert data["project_id"] == subscription.project_id
    assert data["time_window"] == int(subscription.time_window.total_seconds())
    assert data["resolution"] == int(subscription.resolution.total_seconds())
    assert data["query"] == subscription.query


def test_encode_delegate() -> None:
    codec = SubscriptionDataCodec()
    subscription = build_delegate_subscription_data()

    payload = codec.encode(subscription)
    data = json.loads(payload.decode("utf-8"))
    assert data["project_id"] == subscription.project_id
    assert data["time_window"] == int(subscription.time_window.total_seconds())
    assert data["resolution"] == int(subscription.resolution.total_seconds())
    assert data["conditions"] == subscription.conditions
    assert data["aggregations"] == subscription.aggregations
    assert data["query"] == subscription.query


def test_decode() -> None:
    codec = SubscriptionDataCodec()
    subscription = build_legacy_subscription_data()
    data = {
        "project_id": subscription.project_id,
        "conditions": subscription.conditions,
        "aggregations": subscription.aggregations,
        "time_window": int(subscription.time_window.total_seconds()),
        "resolution": int(subscription.resolution.total_seconds()),
    }
    payload = json.dumps(data).encode("utf-8")
    assert codec.decode(payload) == subscription


def test_decode_snql() -> None:
    codec = SubscriptionDataCodec()
    subscription = build_snql_subscription_data()
    data = {
        "type": SubscriptionType.SNQL.value,
        "project_id": subscription.project_id,
        "time_window": int(subscription.time_window.total_seconds()),
        "resolution": int(subscription.resolution.total_seconds()),
        "query": subscription.query,
    }
    payload = json.dumps(data).encode("utf-8")
    assert codec.decode(payload) == subscription


def test_decode_delegate() -> None:
    codec = SubscriptionDataCodec()
    subscription = build_delegate_subscription_data()
    data = {
        "type": SubscriptionType.DELEGATE.value,
        "project_id": subscription.project_id,
        "time_window": int(subscription.time_window.total_seconds()),
        "resolution": int(subscription.resolution.total_seconds()),
        "conditions": subscription.conditions,
        "aggregations": subscription.aggregations,
        "query": subscription.query,
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
