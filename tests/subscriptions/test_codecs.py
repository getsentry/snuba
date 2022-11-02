from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Callable, Optional, Type

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entity_subscriptions.entity_subscription import (
    EntitySubscription,
    EventsSubscription,
    MetricsCountersSubscription,
    MetricsSetsSubscription,
)
from snuba.datasets.entity_subscriptions.factory import get_entity_subscription
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
from tests.datasets.configuration.utils import ConfigurationTest
from tests.subscriptions.subscriptions_utils import create_entity_subscription


def build_snql_subscription_data(
    entity_key: EntityKey,
    organization: Optional[int] = None,
) -> SubscriptionData:

    return SubscriptionData(
        project_id=5,
        time_window_sec=500 * 60,
        resolution_sec=60,
        query="MATCH events SELECT count() WHERE in(platform, 'a')",
        organization=organization,
        entity_subscription=create_entity_subscription(entity_key),
    )


SNQL_CASES = [
    pytest.param(
        build_snql_subscription_data,
        None,
        EntityKey.EVENTS,
        id="snql",
    ),
    pytest.param(
        build_snql_subscription_data,
        1,
        EntityKey.METRICS_COUNTERS,
        id="snql",
    ),
    pytest.param(
        build_snql_subscription_data,
        1,
        EntityKey.METRICS_SETS,
        id="snql",
    ),
]

METRICS_CASES = [
    pytest.param(
        MetricsCountersSubscription,
        "sum",
        EntityKey.METRICS_COUNTERS,
        1,
        id="metrics_counters subscription",
    ),
    pytest.param(
        MetricsSetsSubscription,
        "uniq",
        EntityKey.METRICS_SETS,
        1,
        id="metrics_sets subscription",
    ),
]


def assert_entity_subscription_on_subscription_class(
    subscription: SubscriptionData,
    entity_key: EntityKey,
) -> None:
    subscription_cls = get_entity_subscription(entity_key)
    assert isinstance(subscription.entity_subscription, subscription_cls)


class TestSubscriptionCodecs(ConfigurationTest):
    @pytest.mark.parametrize("builder, organization, entity_key", SNQL_CASES)
    def test_basic(
        self,
        builder: Callable[[EntityKey, Optional[int]], SubscriptionData],
        organization: Optional[int],
        entity_key: EntityKey,
    ) -> None:
        codec = SubscriptionDataCodec(entity_key)
        data = builder(entity_key, organization)
        assert codec.decode(codec.encode(data)) == data

    @pytest.mark.parametrize("builder, organization, entity_key", SNQL_CASES)
    def test_encode_snql(
        self,
        builder: Callable[[EntityKey, Optional[int]], SubscriptionData],
        organization: Optional[int],
        entity_key: EntityKey,
    ) -> None:
        codec = SubscriptionDataCodec(entity_key)
        subscription = builder(entity_key, organization)

        payload = codec.encode(subscription)
        data = json.loads(payload.decode("utf-8"))
        assert data["project_id"] == subscription.project_id
        assert data["time_window"] == subscription.time_window_sec
        assert data["resolution"] == subscription.resolution_sec
        assert data["query"] == subscription.query
        assert data["organization"] == subscription.organization
        assert_entity_subscription_on_subscription_class(subscription, entity_key)

    @pytest.mark.parametrize("builder, organization, entity_key", SNQL_CASES)
    def test_decode_snql(
        self,
        builder: Callable[[EntityKey, Optional[int]], SubscriptionData],
        organization: Optional[int],
        entity_key: EntityKey,
    ) -> None:
        codec = SubscriptionDataCodec(entity_key)
        subscription = builder(entity_key, organization)
        data = {
            "project_id": subscription.project_id,
            "time_window": subscription.time_window_sec,
            "resolution": subscription.resolution_sec,
            "query": subscription.query,
            "organization": subscription.organization,
        }
        payload = json.dumps(data).encode("utf-8")
        assert codec.decode(payload) == subscription

    def test_subscription_task_result_encoder(self) -> None:
        codec = SubscriptionTaskResultEncoder()

        timestamp = datetime.now()

        entity_subscription = EventsSubscription()
        subscription_data = SubscriptionData(
            project_id=1,
            query="MATCH (events) SELECT count() AS count",
            time_window_sec=60,
            resolution_sec=60,
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
        assert data["version"] == 3
        payload = data["payload"]

        assert payload["subscription_id"] == str(
            task_result.task.task.subscription.identifier
        )
        assert payload["request"] == request.original_body
        assert payload["result"] == result
        assert payload["timestamp"] == task_result.task.timestamp.isoformat()
        assert payload["entity"] == EntityKey.EVENTS.value

    @pytest.mark.parametrize(
        "subscription_cls, aggregate, entity_key, organization", METRICS_CASES
    )
    def test_metrics_subscription_task_result_encoder(
        self,
        subscription_cls: Type[EntitySubscription],
        aggregate: str,
        entity_key: EntityKey,
        organization: Optional[int],
    ) -> None:
        codec = SubscriptionTaskResultEncoder()

        timestamp = datetime.now()

        entity_subscription = subscription_cls()
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
            organization=organization,
            entity_subscription=entity_subscription,
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
        assert payload["result"] == result
        assert payload["timestamp"] == task_result.task.timestamp.isoformat()
        assert payload["entity"] == entity_key.value

    def test_subscription_task_encoder(self) -> None:
        encoder = SubscriptionScheduledTaskEncoder()

        subscription_data = SubscriptionData(
            project_id=1,
            query="MATCH events SELECT count()",
            time_window_sec=60,
            resolution_sec=60,
            entity_subscription=EventsSubscription(),
        )

        subscription_id = uuid.UUID("91b46cb6224f11ecb2ddacde48001122")

        epoch = datetime(1970, 1, 1)

        tick_upper_offset = 5

        subscription_with_metadata = SubscriptionWithMetadata(
            EntityKey.EVENTS,
            Subscription(
                SubscriptionIdentifier(PartitionId(1), subscription_id),
                subscription_data,
            ),
            tick_upper_offset,
        )

        task = ScheduledSubscriptionTask(
            timestamp=epoch, task=subscription_with_metadata
        )

        encoded = encoder.encode(task)

        assert encoded.key == b"1/91b46cb6224f11ecb2ddacde48001122"

        assert encoded.value == (
            b"{"
            b'"timestamp":"1970-01-01T00:00:00",'
            b'"entity":"events",'
            b'"task":{'
            b'"data":{"project_id":1,"time_window":60,"resolution":60,"query":"MATCH events SELECT count()","organization":null}},'
            b'"tick_upper_offset":5'
            b"}"
        )

        decoded = encoder.decode(encoded)

        assert decoded == task
