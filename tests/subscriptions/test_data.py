from datetime import datetime
from typing import Optional, Type

import pytest
from sentry_protos.snuba.v1.endpoint_create_subscription_pb2 import (
    CreateSubscriptionRequest as CreateSubscriptionRequestProto,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
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

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.exceptions import InvalidQueryException
from snuba.subscriptions.data import (
    RPCSubscriptionData,
    SnQLSubscriptionData,
    SubscriptionData,
)
from snuba.utils.metrics.timer import Timer
from tests.subscriptions import BaseSubscriptionTest

TESTS = [
    pytest.param(
        SnQLSubscriptionData(
            project_id=1,
            query=(
                "MATCH (events) "
                "SELECT count() AS count "
                "WHERE "
                "platform IN tuple('a') "
            ),
            time_window_sec=500 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.EVENTS),
            metadata={},
        ),
        10,
        None,
        id="SnQL subscription",
    ),
    pytest.param(
        SnQLSubscriptionData(
            project_id=1,
            query=(
                "MATCH (events: events) -[attributes]-> (ga: group_attributes) "
                "SELECT count() AS count "
                "WHERE events.platform IN tuple('a') AND ga.group_status IN array(0)"
            ),
            time_window_sec=500 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.EVENTS),
            metadata={},
        ),
        10,
        None,
        id="SnQL subscription",
    ),
    pytest.param(
        SnQLSubscriptionData(
            project_id=1,
            query=(
                "MATCH (events) "
                "SELECT count() AS count, avg(timestamp) AS average_t "
                "WHERE "
                "platform IN tuple('a') "
            ),
            time_window_sec=500 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.EVENTS),
            metadata={},
        ),
        None,
        InvalidQueryException,
        id="SnQL subscription with 2 many aggregates",
    ),
    pytest.param(
        SnQLSubscriptionData(
            project_id=1,
            query=(
                "MATCH (events) "
                "SELECT count() AS count BY project_id "
                "WHERE platform IN tuple('a') "
                "AND project_id IN tuple(1) "
            ),
            time_window_sec=500 * 60,
            resolution_sec=60,
            entity=get_entity(EntityKey.EVENTS),
            metadata={},
        ),
        None,
        InvalidQueryException,
        id="SnQL subscription with disallowed clause",
    ),
    pytest.param(
        RPCSubscriptionData.from_proto(
            CreateSubscriptionRequestProto(
                time_series_request=TimeSeriesRequest(
                    meta=RequestMeta(
                        project_ids=[1],
                        organization_id=1,
                        cogs_category="something",
                        referrer="something",
                        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    ),
                    aggregations=[
                        AttributeAggregation(
                            aggregate=Function.FUNCTION_COUNT,
                            key=AttributeKey(
                                type=AttributeKey.TYPE_FLOAT, name="my.float.field"
                            ),
                            label="count",
                            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                        ),
                    ],
                ),
                time_window_secs=10800,
                resolution_secs=60,
            ),
            EntityKey.EAP_SPANS,
        ),
        20.0,
        None,
        id="RPC subscription",
    ),
    pytest.param(
        RPCSubscriptionData.from_proto(
            CreateSubscriptionRequestProto(
                time_series_request=TimeSeriesRequest(
                    meta=RequestMeta(
                        project_ids=[1],
                        organization_id=1,
                        cogs_category="something",
                        referrer="something",
                        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    ),
                    aggregations=[
                        AttributeAggregation(
                            aggregate=Function.FUNCTION_COUNT,
                            key=AttributeKey(
                                type=AttributeKey.TYPE_FLOAT, name="my.float.field"
                            ),
                            label="count",
                            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                        ),
                    ],
                    filter=TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(
                                type=AttributeKey.TYPE_STRING, name="sentry.sdk.version"
                            ),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_str="3.0.0"),
                        )
                    ),
                ),
                time_window_secs=3600,
                resolution_secs=60,
            ),
            EntityKey.EAP_SPANS,
        ),
        None,
        None,
        id="RPC subscription with filter",
    ),
]


class TestBuildRequestBase:
    dataset: Dataset

    def compare_conditions(
        self,
        subscription: SubscriptionData,
        exception: Optional[Type[Exception]],
        aggregate: str,
        value: Optional[int | float],
    ) -> None:
        timer = Timer("test")
        if exception is not None:
            with pytest.raises(exception):
                request = subscription.build_request(
                    self.dataset,
                    datetime.utcnow(),
                    100,
                    timer,
                )
                subscription.run_query(self.dataset, request, timer)  # type: ignore
            return

        request = subscription.build_request(
            self.dataset,
            datetime.utcnow(),
            100,
            timer,
        )
        result = subscription.run_query(self.dataset, request, timer)  # type: ignore

        assert result.result["data"][0][aggregate] == value


class TestBuildRequest(BaseSubscriptionTest, TestBuildRequestBase):
    @pytest.mark.parametrize("subscription, expected_value, exception", TESTS)
    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    def test_conditions(
        self,
        subscription: SubscriptionData,
        expected_value: Optional[int | float],
        exception: Optional[Type[Exception]],
    ) -> None:
        self.compare_conditions(subscription, exception, "count", expected_value)
