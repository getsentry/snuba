from copy import deepcopy
from unittest import mock
from unittest.mock import MagicMock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType

from snuba.downsampled_storage_tiers import Tier
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException, QueryResult
from snuba.web.rpc.v1.resolvers.R_eap_items.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    ClickhouseQuerySettings,
    RoutingContext,
)


def _get_in_msg() -> TimeSeriesRequest:
    ts = Timestamp()
    ts.GetCurrentTime()
    tstart = Timestamp(seconds=ts.seconds - 3600)

    return TimeSeriesRequest(
        meta=RequestMeta(
            project_ids=[1, 2, 3],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            start_timestamp=tstart,
            end_timestamp=ts,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        ),
        granularity_secs=60,
    )


def get_query_result() -> QueryResult:
    return QueryResult(
        result={"profile": {"bytes": 420}, "data": [{"row": ["a", "b", "c"]}]},
        extra={"stats": {}, "sql": "", "experiments": {}},
    )


class RoutingStrategyFailsToSelectTier(BaseRoutingStrategy):
    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        raise Exception

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        return get_query_result()

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


class RoutingStrategySelectsTier8(BaseRoutingStrategy):
    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        return Tier.TIER_8, {}

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        return get_query_result()

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


class RoutingStrategyUpdatesQuerySettings(BaseRoutingStrategy):
    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        return Tier.TIER_8, {"some_setting": "some_value"}

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        return get_query_result()

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


class RoutingStrategyBadMetrics(BaseRoutingStrategy):
    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        return Tier.TIER_8, {"some_setting": "some_value"}

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        return get_query_result()

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        if 1 / 0 > 10:
            return


class RoutingStrategyQueryFails(BaseRoutingStrategy):
    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        return Tier.TIER_8, {"some_setting": "some_value"}

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        raise QueryException("this query failed")

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        raise ValueError("should never get here")


ROUTING_CONTEXT = RoutingContext(
    in_msg=_get_in_msg(),
    timer=Timer("stuff"),
    build_query=MagicMock(),
    query_settings=HTTPQuerySettings(),
    query_result=MagicMock(spec=QueryResult),
    extra_info={},
)


def test_target_tier_is_tier_1_if_routing_strategy_fails_to_decide_tier() -> None:
    with mock.patch("snuba.settings.RAISE_ON_ROUTING_STRATEGY_FAILURES", False):
        routing_context = deepcopy(ROUTING_CONTEXT)
        RoutingStrategyFailsToSelectTier().run_query_to_correct_tier(routing_context)
        assert routing_context.query_settings.get_sampling_tier() == Tier.TIER_1


def test_target_tier_is_set_in_routing_context() -> None:
    routing_context = deepcopy(ROUTING_CONTEXT)
    RoutingStrategySelectsTier8().run_query_to_correct_tier(routing_context)
    assert routing_context.query_settings.get_sampling_tier() == Tier.TIER_8


def test_merge_query_settings() -> None:
    routing_context = deepcopy(ROUTING_CONTEXT)
    RoutingStrategyUpdatesQuerySettings().run_query_to_correct_tier(routing_context)
    assert routing_context.query_settings.get_sampling_tier() == Tier.TIER_8
    assert routing_context.query_settings.get_clickhouse_settings() == {
        "some_setting": "some_value"
    }


def test_outputting_metrics_fails_open() -> None:
    with mock.patch("snuba.settings.RAISE_ON_ROUTING_STRATEGY_FAILURES", False):
        routing_context = deepcopy(ROUTING_CONTEXT)
        RoutingStrategyBadMetrics().run_query_to_correct_tier(routing_context)


def test_failed_query() -> None:
    routing_context = deepcopy(ROUTING_CONTEXT)
    with pytest.raises(QueryException):
        RoutingStrategyQueryFails().run_query_to_correct_tier(routing_context)


def test_metrics_output() -> None:
    metric = 0

    class MetricsStrategy(RoutingStrategySelectsTier8):
        def _output_metrics(self, routing_context: RoutingContext) -> None:
            nonlocal metric
            res = super()._output_metrics(routing_context)
            metric += 1
            return res

    routing_context = deepcopy(ROUTING_CONTEXT)
    with mock.patch(
        "snuba.web.rpc.v1.resolvers.R_eap_items.routing_strategies.storage_routing.record_query"
    ) as record_query:
        # with mock.patch("snuba.state.record_query") as record_query:
        MetricsStrategy().run_query_to_correct_tier(routing_context)
        record_query.assert_called_once()
        assert metric == 1
