from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from typing import Callable, Optional

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


def build_legacy_subscription_data(organization=None) -> LegacySubscriptionData:
    legacy_subs_dict = {
        "project_id": 5,
        "conditions": [["platform", "IN", ["a"]]],
        "aggregations": [["count()", "", "count"]],
        "time_window": timedelta(minutes=500),
        "resolution": timedelta(minutes=1),
    }
    if organization:
        legacy_subs_dict.update({"organization": organization})
    return LegacySubscriptionData(**legacy_subs_dict)


def build_snql_subscription_data(organization=None) -> SnQLSubscriptionData:
    snql_subs_dict = {
        "project_id": 5,
        "time_window": timedelta(minutes=500),
        "resolution": timedelta(minutes=1),
        "query": "MATCH events SELECT count() WHERE in(platform, 'a')",
    }
    if organization:
        snql_subs_dict.update({"organization": organization})
    return SnQLSubscriptionData(**snql_subs_dict)


def build_delegate_subscription_data(organization=None) -> DelegateSubscriptionData:
    delegate_subs_dict = {
        "project_id": 5,
        "time_window": timedelta(minutes=500),
        "resolution": timedelta(minutes=1),
        "conditions": [["platform", "IN", ["a"]]],
        "aggregations": [["count()", "", "count"]],
        "query": "MATCH events SELECT count() WHERE in(platform, 'a')",
    }
    if organization:
        delegate_subs_dict.update({"organization": organization})
    return DelegateSubscriptionData(**delegate_subs_dict)


LEGACY_CASES = [
    pytest.param(build_legacy_subscription_data, None, id="legacy"),
    pytest.param(build_legacy_subscription_data, 1, id="legacy"),
]

SNQL_CASES = [
    pytest.param(build_snql_subscription_data, None, id="snql",),
    pytest.param(build_snql_subscription_data, 1, id="snql",),
]

DELEGATE_CASES = [
    pytest.param(build_delegate_subscription_data, None, id="delegate"),
    pytest.param(build_delegate_subscription_data, 1, id="delegate"),
]


@pytest.mark.parametrize(
    "builder, organization", [*LEGACY_CASES, *SNQL_CASES, *DELEGATE_CASES]
)
def test_basic(
    builder: Callable[[Optional[int]], SubscriptionData], organization: Optional[int]
) -> None:
    codec = SubscriptionDataCodec()
    data = builder(organization)
    assert codec.decode(codec.encode(data)) == data


@pytest.mark.parametrize("builder, organization", LEGACY_CASES)
def test_encode(
    builder: Callable[[Optional[int]], LegacySubscriptionData],
    organization: Optional[int],
) -> None:
    codec = SubscriptionDataCodec()
    subscription = builder(organization)

    payload = codec.encode(subscription)
    data = json.loads(payload.decode("utf-8"))
    assert data["project_id"] == subscription.project_id
    assert data["conditions"] == subscription.conditions
    assert data["aggregations"] == subscription.aggregations
    assert data["time_window"] == int(subscription.time_window.total_seconds())
    assert data["resolution"] == int(subscription.resolution.total_seconds())

    if organization:
        assert data["organization"] == subscription.organization


@pytest.mark.parametrize("builder, organization", SNQL_CASES)
def test_encode_snql(
    builder: Callable[[Optional[int]], SnQLSubscriptionData],
    organization: Optional[int],
) -> None:
    codec = SubscriptionDataCodec()
    subscription = builder(organization)

    payload = codec.encode(subscription)
    data = json.loads(payload.decode("utf-8"))
    assert data["project_id"] == subscription.project_id
    assert data["time_window"] == int(subscription.time_window.total_seconds())
    assert data["resolution"] == int(subscription.resolution.total_seconds())
    assert data["query"] == subscription.query

    if organization:
        assert data["organization"] == subscription.organization


@pytest.mark.parametrize("builder, organization", DELEGATE_CASES)
def test_encode_delegate(
    builder: Callable[[Optional[int]], DelegateSubscriptionData],
    organization: Optional[int],
) -> None:
    codec = SubscriptionDataCodec()
    subscription = builder(organization)

    payload = codec.encode(subscription)
    data = json.loads(payload.decode("utf-8"))
    assert data["project_id"] == subscription.project_id
    assert data["time_window"] == int(subscription.time_window.total_seconds())
    assert data["resolution"] == int(subscription.resolution.total_seconds())
    assert data["conditions"] == subscription.conditions
    assert data["aggregations"] == subscription.aggregations
    assert data["query"] == subscription.query

    if organization:
        assert data["organization"] == subscription.organization


@pytest.mark.parametrize("builder, organization", LEGACY_CASES)
def test_decode(
    builder: Callable[[Optional[int]], LegacySubscriptionData],
    organization: Optional[int],
) -> None:
    codec = SubscriptionDataCodec()
    subscription = builder(organization)
    data = {
        "project_id": subscription.project_id,
        "conditions": subscription.conditions,
        "aggregations": subscription.aggregations,
        "time_window": int(subscription.time_window.total_seconds()),
        "resolution": int(subscription.resolution.total_seconds()),
    }
    if organization:
        data.update({"organization": organization})
    payload = json.dumps(data).encode("utf-8")
    assert codec.decode(payload) == subscription


@pytest.mark.parametrize("builder, organization", SNQL_CASES)
def test_decode_snql(
    builder: Callable[[Optional[int]], SnQLSubscriptionData],
    organization: Optional[int],
) -> None:
    codec = SubscriptionDataCodec()
    subscription = builder(organization)
    data = {
        "type": SubscriptionType.SNQL.value,
        "project_id": subscription.project_id,
        "time_window": int(subscription.time_window.total_seconds()),
        "resolution": int(subscription.resolution.total_seconds()),
        "query": subscription.query,
    }
    if organization:
        data.update({"organization": organization})
    payload = json.dumps(data).encode("utf-8")
    assert codec.decode(payload) == subscription


@pytest.mark.parametrize("builder, organization", DELEGATE_CASES)
def test_decode_delegate(
    builder: Callable[[Optional[int]], DelegateSubscriptionData],
    organization: Optional[int],
) -> None:
    codec = SubscriptionDataCodec()
    subscription = builder(organization)
    data = {
        "type": SubscriptionType.DELEGATE.value,
        "project_id": subscription.project_id,
        "time_window": int(subscription.time_window.total_seconds()),
        "resolution": int(subscription.resolution.total_seconds()),
        "conditions": subscription.conditions,
        "aggregations": subscription.aggregations,
        "query": subscription.query,
    }
    if organization:
        data.update({"organization": organization})
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
