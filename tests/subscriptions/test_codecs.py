from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from typing import Callable, Optional

import pytest

from snuba.datasets.entities import EntityKey
from snuba.datasets.factory import get_dataset
from snuba.reader import Result
from snuba.subscriptions.codecs import (
    SubscriptionDataCodec,
    SubscriptionScheduledTaskEncoder,
    SubscriptionTaskResultEncoder,
)
from snuba.subscriptions.data import (
    PartitionId,
    ScheduledSubscriptionTask,
    SnQLSubscriptionData,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
    SubscriptionWithMetadata,
)
from snuba.subscriptions.entity_subscription import (
    EntitySubscription,
    EventsSubscription,
    SessionsSubscription,
    SubscriptionType,
)
from snuba.subscriptions.worker import SubscriptionTaskResult
from snuba.utils.metrics.timer import Timer


def build_snql_subscription_data(
    organization: Optional[int] = None,
) -> SnQLSubscriptionData:
    entity_subscription: EntitySubscription
    if not organization:
        entity_subscription = EventsSubscription(data_dict={})
    else:
        entity_subscription = SessionsSubscription(
            data_dict={"organization": organization},
        )
    return SnQLSubscriptionData(
        project_id=5,
        time_window=timedelta(minutes=500),
        resolution=timedelta(minutes=1),
        query="MATCH events SELECT count() WHERE in(platform, 'a')",
        entity_subscription=entity_subscription,
    )


SNQL_CASES = [
    pytest.param(build_snql_subscription_data, None, id="snql",),
    pytest.param(build_snql_subscription_data, 1, id="snql",),
]


def assert_entity_subscription_on_subscription_class(
    organization: Optional[int], subscription: SubscriptionData,
) -> None:
    if organization:
        assert isinstance(subscription.entity_subscription, SessionsSubscription)
        assert subscription.entity_subscription.organization == organization
    else:
        assert isinstance(subscription.entity_subscription, EventsSubscription)
        with pytest.raises(AttributeError):
            getattr(subscription.entity_subscription, "organization")


@pytest.mark.parametrize("builder, organization", [*SNQL_CASES])
def test_basic(
    builder: Callable[[Optional[int]], SubscriptionData], organization: Optional[int]
) -> None:
    entity = EntityKey.SESSIONS if organization else EntityKey.EVENTS
    codec = SubscriptionDataCodec(entity)
    data = builder(organization)
    assert codec.decode(codec.encode(data)) == data


@pytest.mark.parametrize("builder, organization", SNQL_CASES)
def test_encode_snql(
    builder: Callable[[Optional[int]], SnQLSubscriptionData],
    organization: Optional[int],
) -> None:
    entity = EntityKey.SESSIONS if organization else EntityKey.EVENTS
    codec = SubscriptionDataCodec(entity)
    subscription = builder(organization)

    payload = codec.encode(subscription)
    data = json.loads(payload.decode("utf-8"))
    assert data["project_id"] == subscription.project_id
    assert data["time_window"] == int(subscription.time_window.total_seconds())
    assert data["resolution"] == int(subscription.resolution.total_seconds())
    assert data["query"] == subscription.query
    assert_entity_subscription_on_subscription_class(organization, subscription)


@pytest.mark.parametrize("builder, organization", SNQL_CASES)
def test_decode_snql(
    builder: Callable[[Optional[int]], SnQLSubscriptionData],
    organization: Optional[int],
) -> None:
    entity = EntityKey.SESSIONS if organization else EntityKey.EVENTS
    codec = SubscriptionDataCodec(entity)
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


def test_subscription_task_result_encoder() -> None:
    codec = SubscriptionTaskResultEncoder()

    timestamp = datetime.now()

    entity_subscription = EventsSubscription(data_dict={})
    subscription_data = SnQLSubscriptionData(
        project_id=1,
        query="MATCH (events) SELECT count() AS count",
        time_window=timedelta(minutes=1),
        resolution=timedelta(minutes=1),
        entity_subscription=entity_subscription,
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
        ScheduledSubscriptionTask(
            timestamp,
            SubscriptionWithMetadata(
                EntityKey.EVENTS,
                Subscription(
                    SubscriptionIdentifier(PartitionId(1), uuid.uuid1()),
                    subscription_data,
                ),
                5,
            ),
        ),
        (request, result),
    )

    message = codec.encode(task_result)
    data = json.loads(message.value.decode("utf-8"))
    assert data["version"] == 2
    payload = data["payload"]

    assert payload["subscription_id"] == str(
        task_result.task.task.subscription.identifier
    )
    assert payload["request"] == request.body
    assert payload["result"] == result
    assert payload["timestamp"] == task_result.task.timestamp.isoformat()


def test_sessions_subscription_task_result_encoder() -> None:
    codec = SubscriptionTaskResultEncoder()

    timestamp = datetime.now()

    entity_subscription = SessionsSubscription(data_dict={"organization": 1})
    subscription_data = SnQLSubscriptionData(
        project_id=1,
        query=(
            """
            MATCH (sessions) SELECT if(greater(sessions,0),
            divide(sessions_crashed,sessions),null)
            AS _crash_rate_alert_aggregate, identity(sessions) AS _total_sessions
            WHERE org_id = 1 AND project_id IN tuple(1) LIMIT 1
            OFFSET 0 GRANULARITY 3600
            """
        ),
        time_window=timedelta(minutes=1),
        resolution=timedelta(minutes=1),
        entity_subscription=entity_subscription,
    )

    # XXX: This seems way too coupled to the dataset.
    request = subscription_data.build_request(
        get_dataset("sessions"), timestamp, None, Timer("timer")
    )
    result: Result = {
        "meta": [
            {"type": "UInt64", "name": "_total_sessions"},
            {"name": "_crash_rate_alert_aggregate", "type": "Nullable(Float64)"},
        ],
        "data": [{"_crash_rate_alert_aggregate": 0.0, "_total_sessions": 25}],
    }

    task_result = SubscriptionTaskResult(
        ScheduledSubscriptionTask(
            timestamp,
            SubscriptionWithMetadata(
                EntityKey.EVENTS,
                Subscription(
                    SubscriptionIdentifier(PartitionId(1), uuid.uuid1()),
                    subscription_data,
                ),
                5,
            ),
        ),
        (request, result),
    )

    message = codec.encode(task_result)
    data = json.loads(message.value.decode("utf-8"))
    assert data["version"] == 2
    payload = data["payload"]

    assert payload["subscription_id"] == str(
        task_result.task.task.subscription.identifier
    )
    assert payload["request"] == request.body
    assert payload["result"] == result
    assert payload["timestamp"] == task_result.task.timestamp.isoformat()


def test_subscription_task_encoder() -> None:
    encoder = SubscriptionScheduledTaskEncoder()

    subscription_data = SnQLSubscriptionData(
        project_id=1,
        query="MATCH events SELECT count()",
        time_window=timedelta(minutes=1),
        resolution=timedelta(minutes=1),
        entity_subscription=EventsSubscription(data_dict={}),
    )

    subscription_id = uuid.UUID("91b46cb6224f11ecb2ddacde48001122")

    epoch = datetime(1970, 1, 1)

    tick_upper_offset = 5

    subscription_with_metadata = SubscriptionWithMetadata(
        EntityKey.EVENTS,
        Subscription(
            SubscriptionIdentifier(PartitionId(1), subscription_id), subscription_data
        ),
        tick_upper_offset,
    )

    task = ScheduledSubscriptionTask(timestamp=epoch, task=subscription_with_metadata)

    encoded = encoder.encode(task)

    assert encoded.key == b"1/91b46cb6224f11ecb2ddacde48001122"

    assert encoded.value == (
        b"{"
        b'"timestamp":28800.0,'
        b'"entity":"events",'
        b'"task":{'
        b'"data":{"type":"snql","project_id":1,"time_window":60,"resolution":60,"query":"MATCH events SELECT count()"}},'
        b'"tick_upper_offset":5'
        b"}"
    )

    decoded = encoder.decode(encoded)

    assert decoded == task
