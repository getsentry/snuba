import importlib
import uuid
from datetime import timedelta
from random import randint
from typing import MutableSequence
from unittest.mock import patch

import pytest
from sentry_protos.snuba.v1.endpoint_create_subscription_pb2 import (
    CreateSubscriptionRequest as CreateSubscriptionRequestProto,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    AttributeValue,
    ExtrapolationMode,
    Function,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.subscriptions import scheduler
from snuba.subscriptions.data import (
    PartitionId,
    RPCSubscriptionData,
    SnQLSubscriptionData,
    Subscription,
    SubscriptionIdentifier,
)
from snuba.subscriptions.scheduler import filter_subscriptions
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend


def build_subscription(resolution: timedelta, org_id: int) -> Subscription:
    return Subscription(
        SubscriptionIdentifier(PartitionId(1), uuid.uuid4()),
        SnQLSubscriptionData(
            project_id=1,
            query="MATCH (events) SELECT count() AS count",
            time_window_sec=60,
            resolution_sec=int(resolution.total_seconds()),
            entity=get_entity(EntityKey.EVENTS),
            metadata={"organization": org_id},
        ),
    )


@pytest.fixture
def expected_subs() -> MutableSequence[Subscription]:
    return [
        build_subscription(timedelta(minutes=1), 2) for count in range(randint(1, 50))
    ]


@pytest.fixture
def extra_subs() -> MutableSequence[Subscription]:
    return [
        build_subscription(timedelta(minutes=3), 1) for count in range(randint(1, 50))
    ]


@patch("snuba.settings.SLICED_STORAGE_SETS", {"events": 3})
@patch("snuba.settings.LOGICAL_PARTITION_MAPPING", {"events": {0: 0, 1: 1, 2: 2}})
def test_filter_subscriptions(expected_subs, extra_subs) -> None:  # type: ignore
    importlib.reload(scheduler)

    filtered_subs = filter_subscriptions(
        subscriptions=expected_subs + extra_subs,
        entity_key=EntityKey.EVENTS,
        metrics=DummyMetricsBackend(strict=True),
        slice_id=2,
    )
    assert filtered_subs == expected_subs


def build_rpc_subscription(resolution: timedelta, org_id: int) -> Subscription:
    return Subscription(
        SubscriptionIdentifier(PartitionId(1), uuid.uuid4()),
        RPCSubscriptionData.from_proto(
            CreateSubscriptionRequestProto(
                time_series_request=TimeSeriesRequest(
                    meta=RequestMeta(
                        project_ids=[1, 2, 3],
                        organization_id=org_id,
                        cogs_category="something",
                        referrer="something",
                    ),
                    aggregations=[
                        AttributeAggregation(
                            aggregate=Function.FUNCTION_SUM,
                            key=AttributeKey(
                                type=AttributeKey.TYPE_FLOAT, name="test_metric"
                            ),
                            label="sum",
                            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                        ),
                    ],
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="foo"),
                            op=ComparisonFilter.OP_NOT_EQUALS,
                            value=AttributeValue(val_str="bar"),
                        )
                    ),
                    granularity_secs=300,
                ),
                time_window_secs=300,
                resolution_secs=int(resolution.total_seconds()),
            ),
            EntityKey.EAP_SPANS,
        ),
    )


@pytest.fixture
def expected_rpc_subs() -> MutableSequence[Subscription]:
    return [
        build_rpc_subscription(timedelta(minutes=1), 2)
        for count in range(randint(1, 50))
    ]


@pytest.fixture
def extra_rpc_subs() -> MutableSequence[Subscription]:
    return [
        build_rpc_subscription(timedelta(minutes=3), 1)
        for count in range(randint(1, 50))
    ]


@patch("snuba.settings.SLICED_STORAGE_SETS", {"events_analytics_platform": 3})
@patch(
    "snuba.settings.LOGICAL_PARTITION_MAPPING",
    {"events_analytics_platform": {0: 0, 1: 1, 2: 2}},
)
def test_filter_rpc_subscriptions(expected_rpc_subs, extra_rpc_subs) -> None:  # type: ignore
    importlib.reload(scheduler)

    filtered_subs = filter_subscriptions(
        subscriptions=expected_rpc_subs + extra_rpc_subs,
        entity_key=EntityKey.EAP_SPANS,
        metrics=DummyMetricsBackend(strict=True),
        slice_id=2,
    )
    assert filtered_subs == expected_rpc_subs
