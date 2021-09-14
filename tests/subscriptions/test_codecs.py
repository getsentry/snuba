from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, MutableMapping, Union

import pytest

from snuba.datasets.factory import get_dataset
from snuba.reader import Result
from snuba.subscriptions.codecs import (
    SubscriptionDataCodec,
    SubscriptionTaskResultEncoder,
)
from snuba.subscriptions.data import (
    CRASH_RATE_ALERT_PATTERN,
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


def build_legacy_sessions_subscription_data() -> LegacySubscriptionData:
    return LegacySubscriptionData(
        project_id=1,
        conditions=[],
        aggregations=[
            [
                "multiply(minus(1, divide(sessions_crashed, sessions)), 100)",
                None,
                "crash_free_percentage",
            ]
        ],
        time_window=timedelta(minutes=120),
        resolution=timedelta(minutes=1),
        organization=1,
        limit=1,
        offset=0,
        granularity=60,
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


def build_delegate_sessions_subscription_data() -> DelegateSubscriptionData:
    return DelegateSubscriptionData(
        project_id=1,
        query=(
            "MATCH (sessions) "
            "SELECT multiply(minus(1, divide(sessions_crashed, sessions)), 100) "
            "AS crash_free_percentage "
            "WHERE org_id = 1 AND project_id IN tuple(1) "
            "LIMIT 1 "
            "OFFSET 0 "
            "GRANULARITY 3600"
        ),
        conditions=[],
        aggregations=[
            [
                "multiply(minus(1, divide(sessions_crashed, sessions)), 100)",
                None,
                "crash_free_percentage",
            ]
        ],
        time_window=timedelta(minutes=120),
        resolution=timedelta(minutes=1),
        organization=1,
        limit=1,
        offset=0,
        granularity=60,
    )


LEGACY_CASES = [
    pytest.param(build_legacy_subscription_data, None, id="legacy"),
    pytest.param(
        build_legacy_sessions_subscription_data, "crash_free_percentage", id="legacy"
    ),
]

DELEGATE_CASES = [
    pytest.param(build_delegate_subscription_data, None, id="delegate"),
    pytest.param(
        build_delegate_sessions_subscription_data,
        "crash_free_percentage",
        id="delegate",
    ),
]

BASIC_CASES = [
    *LEGACY_CASES,
    pytest.param(build_snql_subscription_data, None, id="snql"),
    *DELEGATE_CASES,
]


def check_sessions_subscription_fields(
    data: MutableMapping[Any],
    subscription: Union[LegacySubscriptionData, DelegateSubscriptionData],
    aggregate: str,
) -> None:
    """
    Helper function that ensures that columns related to sessions subscriptions are present in
    the subscription object
    """
    if not (aggregate and bool(CRASH_RATE_ALERT_PATTERN.match(aggregate))):
        return

    assert data["organization"] == subscription.organization
    assert data["limit"] == subscription.limit
    assert data["offset"] == subscription.offset
    assert data["granularity"] == subscription.granularity


@pytest.mark.parametrize("builder, _", BASIC_CASES)
def test_basic(builder: Callable[[], SubscriptionData], _: str) -> None:
    codec = SubscriptionDataCodec()
    data = builder()
    assert codec.decode(codec.encode(data)) == data


@pytest.mark.parametrize("builder, aggregate", LEGACY_CASES)
def test_encode(builder: Callable[[], LegacySubscriptionData], aggregate: str) -> None:
    codec = SubscriptionDataCodec()
    subscription: LegacySubscriptionData = builder()

    payload = codec.encode(subscription)
    data = json.loads(payload.decode("utf-8"))
    assert data["project_id"] == subscription.project_id
    assert data["conditions"] == subscription.conditions
    assert data["aggregations"] == subscription.aggregations
    assert data["time_window"] == int(subscription.time_window.total_seconds())
    assert data["resolution"] == int(subscription.resolution.total_seconds())

    check_sessions_subscription_fields(
        data=data, subscription=subscription, aggregate=aggregate
    )


def test_encode_snql() -> None:
    codec = SubscriptionDataCodec()
    subscription = build_snql_subscription_data()

    payload = codec.encode(subscription)
    data = json.loads(payload.decode("utf-8"))
    assert data["project_id"] == subscription.project_id
    assert data["time_window"] == int(subscription.time_window.total_seconds())
    assert data["resolution"] == int(subscription.resolution.total_seconds())
    assert data["query"] == subscription.query


@pytest.mark.parametrize("builder, aggregate", DELEGATE_CASES)
def test_encode_delegate(
    builder: Callable[[], DelegateSubscriptionData], aggregate: str
) -> None:
    codec = SubscriptionDataCodec()
    subscription = builder()

    payload = codec.encode(subscription)
    data = json.loads(payload.decode("utf-8"))
    assert data["project_id"] == subscription.project_id
    assert data["time_window"] == int(subscription.time_window.total_seconds())
    assert data["resolution"] == int(subscription.resolution.total_seconds())
    assert data["conditions"] == subscription.conditions
    assert data["aggregations"] == subscription.aggregations
    assert data["query"] == subscription.query

    check_sessions_subscription_fields(
        data=data, subscription=subscription, aggregate=aggregate
    )


@pytest.mark.parametrize("builder, aggregate", LEGACY_CASES)
def test_decode(builder: Callable[[], LegacySubscriptionData], aggregate: str) -> None:
    codec = SubscriptionDataCodec()
    subscription = builder()
    data = {
        "project_id": subscription.project_id,
        "conditions": subscription.conditions,
        "aggregations": subscription.aggregations,
        "time_window": int(subscription.time_window.total_seconds()),
        "resolution": int(subscription.resolution.total_seconds()),
    }

    if aggregate and bool(CRASH_RATE_ALERT_PATTERN.match(aggregate)):
        data.update(
            {
                "organization": subscription.organization,
                "limit": subscription.limit,
                "offset": subscription.offset,
                "granularity": subscription.granularity,
            }
        )
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


@pytest.mark.parametrize("builder, aggregate", DELEGATE_CASES)
def test_decode_delegate(
    builder: Callable[[], DelegateSubscriptionData], aggregate: str
) -> None:
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

    if aggregate and bool(CRASH_RATE_ALERT_PATTERN.match(aggregate)):
        data.update(
            {
                "organization": subscription.organization,
                "limit": subscription.limit,
                "offset": subscription.offset,
                "granularity": subscription.granularity,
            }
        )
    payload = json.dumps(data).encode("utf-8")
    assert codec.decode(payload) == subscription


@pytest.mark.parametrize("builder, aggregate", DELEGATE_CASES)
def test_subscription_task_result_encoder(
    builder: Callable[[], DelegateSubscriptionData], aggregate: str
) -> None:
    codec = SubscriptionTaskResultEncoder()

    timestamp = datetime.now()

    subscription_data = builder()

    # XXX: This seems way too coupled to the dataset.
    request = subscription_data.build_request(
        get_dataset("events"), timestamp, None, Timer("timer")
    )
    if aggregate and bool(CRASH_RATE_ALERT_PATTERN.match(aggregate)):
        result: Result = {
            "meta": [{"type": "UInt64", "name": "crash_free_percentage"}],
            "data": [{"crash_free_percentage": 95}],
        }
    else:
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
