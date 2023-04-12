from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Callable, Mapping

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity
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
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
    SubscriptionTaskResult,
    SubscriptionWithMetadata,
)
from snuba.utils.metrics.timer import Timer


def build_snql_subscription_data(
    entity_key: EntityKey, metadata: Mapping[str, Any]
) -> SubscriptionData:

    return SubscriptionData(
        project_id=5,
        time_window_sec=500 * 60,
        resolution_sec=60,
        query="MATCH events SELECT count() WHERE in(platform, 'a')",
        entity=get_entity(entity_key),
        metadata=metadata,
    )


SNQL_CASES = [
    pytest.param(
        build_snql_subscription_data,
        {},
        EntityKey.EVENTS,
        id="snql",
    ),
    pytest.param(
        build_snql_subscription_data,
        {"organization": 1},
        EntityKey.METRICS_COUNTERS,
        id="snql",
    ),
    pytest.param(
        build_snql_subscription_data,
        {"organization": 1},
        EntityKey.METRICS_SETS,
        id="snql",
    ),
]


@pytest.mark.parametrize("builder, metadata, entity_key", SNQL_CASES)
def test_basic(
    builder: Callable[[EntityKey, Mapping[str, Any]], SubscriptionData],
    metadata: Mapping[str, Any],
    entity_key: EntityKey,
) -> None:
    codec = SubscriptionDataCodec(entity_key)
    data = builder(entity_key, metadata)
    assert codec.decode(codec.encode(data)) == data


@pytest.mark.parametrize("builder, metadata, entity_key", SNQL_CASES)
def test_encode_snql(
    builder: Callable[[EntityKey, Mapping[str, Any]], SubscriptionData],
    metadata: Mapping[str, Any],
    entity_key: EntityKey,
) -> None:
    codec = SubscriptionDataCodec(entity_key)
    subscription = builder(entity_key, metadata)

    payload = codec.encode(subscription)
    data = json.loads(payload.decode("utf-8"))
    assert data["project_id"] == subscription.project_id
    assert data["time_window"] == subscription.time_window_sec
    assert data["resolution"] == subscription.resolution_sec
    assert data["query"] == subscription.query
    assert metadata == subscription.metadata


@pytest.mark.parametrize("builder, metadata, entity_key", SNQL_CASES)
def test_decode_snql(
    builder: Callable[[EntityKey, Mapping[str, Any]], SubscriptionData],
    metadata: Mapping[str, Any],
    entity_key: EntityKey,
) -> None:
    codec = SubscriptionDataCodec(entity_key)
    subscription = builder(entity_key, metadata)
    data = {
        "project_id": subscription.project_id,
        "time_window": subscription.time_window_sec,
        "resolution": subscription.resolution_sec,
        "query": subscription.query,
    }
    if metadata:
        data.update(metadata)
    payload = json.dumps(data).encode("utf-8")
    assert codec.decode(payload) == subscription


def test_subscription_task_result_encoder() -> None:
    codec = SubscriptionTaskResultEncoder()

    timestamp = datetime.now()

    entity = get_entity(EntityKey.EVENTS)
    subscription_data = SubscriptionData(
        project_id=1,
        query="MATCH (events) SELECT count() AS count",
        time_window_sec=60,
        resolution_sec=60,
        entity=entity,
        metadata={},
    )

    # XXX: This seems way too coupled to the dataset.
    request = subscription_data.build_request(
        get_dataset("events"), timestamp, None, Timer("timer")
    )
    result: Result = {
        "meta": [{"type": "UInt64", "name": "count"}],
        "data": [{"count": 1}],
        "profile": {},
        "trace_output": "",
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
    assert data["version"] == 3
    payload = data["payload"]

    assert payload["subscription_id"] == str(
        task_result.task.task.subscription.identifier
    )
    assert payload["request"] == request.original_body
    assert payload["result"]["data"] == result["data"]
    assert payload["timestamp"] == task_result.task.timestamp.isoformat()
    assert payload["entity"] == EntityKey.EVENTS.value


METRICS_CASES = [
    pytest.param(
        get_entity(EntityKey.METRICS_COUNTERS),
        "sum",
        EntityKey.METRICS_COUNTERS,
        id="metrics_counters subscription",
    ),
    pytest.param(
        get_entity(EntityKey.METRICS_SETS),
        "uniq",
        EntityKey.METRICS_SETS,
        id="metrics_sets subscription",
    ),
]


@pytest.mark.parametrize("entity, aggregate, entity_key", METRICS_CASES)
def test_metrics_subscription_task_result_encoder(
    entity: Entity, aggregate: str, entity_key: EntityKey
) -> None:
    codec = SubscriptionTaskResultEncoder()
    metadata = {"organization": 1}
    timestamp = datetime.now()

    subscription_data = SubscriptionData(
        project_id=1,
        query=(
            f"""
            MATCH ({entity_key.value}) SELECT {aggregate}(value) AS value BY project_id, tags[3]
            WHERE org_id = 1 AND project_id IN array(1) AND metric_id = 7 AND tags[3] IN array(1,2)
            """
        ),
        time_window_sec=60,
        resolution_sec=60,
        entity=entity,
        metadata=metadata,
    )

    # XXX: This seems way too coupled to the dataset.
    request = subscription_data.build_request(
        get_dataset("metrics"), timestamp, None, Timer("timer")
    )
    result: Result = {
        "data": [
            {"project_id": 1, "tags[3]": 13, "value": 8},
            {"project_id": 1, "tags[3]": 4, "value": 46},
        ],
        "meta": [
            {"name": "project_id", "type": "UInt64"},
            {"name": "tags[3]", "type": "UInt64"},
            {"name": "value", "type": "Float64"},
        ],
        "profile": {},
        "trace_output": "",
    }
    task_result = SubscriptionTaskResult(
        ScheduledSubscriptionTask(
            timestamp,
            SubscriptionWithMetadata(
                entity_key,
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
    assert data["version"] == 3
    payload = data["payload"]

    assert payload["subscription_id"] == str(
        task_result.task.task.subscription.identifier
    )
    assert payload["request"] == request.original_body
    assert payload["result"]["data"] == result["data"]
    assert payload["timestamp"] == task_result.task.timestamp.isoformat()
    assert payload["entity"] == entity_key.value


def test_subscription_task_encoder() -> None:
    encoder = SubscriptionScheduledTaskEncoder()
    entity = get_entity(EntityKey.EVENTS)
    subscription_data = SubscriptionData(
        project_id=1,
        query="MATCH events SELECT count()",
        time_window_sec=60,
        resolution_sec=60,
        entity=entity,
        metadata={},
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
        b'"timestamp":"1970-01-01T00:00:00",'
        b'"entity":"events",'
        b'"task":{'
        b'"data":{"project_id":1,"time_window":60,"resolution":60,"query":"MATCH events SELECT count()"}},'
        b'"tick_upper_offset":5'
        b"}"
    )

    decoded = encoder.decode(encoded)

    assert decoded == task
