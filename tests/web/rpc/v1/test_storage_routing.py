import json
import uuid
from copy import deepcopy
from unittest import mock
from unittest.mock import MagicMock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_kafka_schemas import get_codec
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType

from snuba.downsampled_storage_tiers import Tier
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException, QueryResult
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing import (
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
            request_id=str(uuid.uuid4()),
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
        extra={
            "stats": {"stat": 1},
            "sql": "SELECT * FROM your_mom",  # this query times out on our largest cluster
            "experiments": {},
        },
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
            metric += 1
            self._record_value_in_span_and_DD(
                routing_context=routing_context,
                metrics_backend_func=self.metrics.increment,
                name="my_metric",
                value=1,
                tags={"a": "b", "c": "d"},
            )

    routing_context = deepcopy(ROUTING_CONTEXT)
    with mock.patch(
        "snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.record_query"
    ) as record_query:
        result = MetricsStrategy().run_query_to_correct_tier(routing_context)
        record_query.assert_called_once()
        recorded_payload = record_query.mock_calls[0].args[0]
        assert recorded_payload["dataset"] == "storage_routing"
        assert recorded_payload["status"] == "TIER_8"
        assert recorded_payload["request"]["referrer"] == "MetricsStrategy"
        assert recorded_payload["query_list"][0]["stats"] == {
            "extra_info": {
                "sampling_in_storage_estimation_time_overhead": {
                    "type": "timing",
                    "value": 0,  # we decide the tier immediately so timing is very small
                    "tags": None,
                },
                "sampling_in_storage_my_metric": {
                    "type": "increment",
                    "value": 1,
                    "tags": {"a": "b", "c": "d"},
                },
            },
            "clickhouse_settings": {},
            "source_request_id": routing_context.in_msg.meta.request_id,
            "result_info": {
                "meta": {},
                "profile": {"bytes": 420},
                "sql": result.extra["sql"],
                "stats": result.extra["stats"],
            },
            "routed_tier": "TIER_8",
            "final": False,
            "cache_hit": 0,
            "max_threads": routing_context.query_settings.get_clickhouse_settings().get(
                "max_threads", 0
            ),
            "clickhouse_table": "na",
            "query_id": "na",
            "is_duplicate": 0,
            "consistent": False,
        }
        schema = get_codec("snuba-queries")
        payload_bytes = json.dumps(recorded_payload).encode("utf-8")
        schema.decode(payload_bytes)
        assert metric == 1
