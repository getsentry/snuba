import json
import uuid
from copy import deepcopy
from unittest import mock
from unittest.mock import MagicMock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_kafka_schemas import get_codec
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType

from snuba import state
from snuba.configs.configuration import Configuration
from snuba.downsampled_storage_tiers import Tier
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.rpc.storage_routing.common import extract_message_meta
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    RoutingContext,
    RoutingDecision,
)
from snuba.web.rpc.v1.endpoint_time_series import EndpointTimeSeries

RANDOM_REQUEST_ID = str(uuid.uuid4())


def _get_in_msg() -> TimeSeriesRequest:
    ts = Timestamp()
    ts.GetCurrentTime()
    tstart = Timestamp(seconds=ts.seconds - 3600)

    return TimeSeriesRequest(
        meta=RequestMeta(
            request_id=RANDOM_REQUEST_ID,
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


def get_query_result(elapsed_ms: int = 1000) -> QueryResult:
    return QueryResult(
        result={
            "profile": {"bytes": 420, "elapsed": elapsed_ms / 1000},
            "data": [{"row": ["a", "b", "c"]}],
        },
        extra={
            "stats": {"stat": 1},
            "sql": "SELECT * FROM your_mom",  # this query times out on our largest cluster
            "experiments": {},
        },
    )


class RoutingStrategyFailsToSelectTier(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    def _get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        raise Exception

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


class RoutingStrategySelectsTier8(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    def _get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        return RoutingDecision(
            routing_context=routing_context,
            strategy=self,
            tier=Tier.TIER_8,
            can_run=True,
            clickhouse_settings={},
        )

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


# a routing strategy that returns tier 1 if the tier is highest accuracy otherwise tier 8
class RoutingStrategyHighestAccuracy(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    def _get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        if self._is_highest_accuracy_mode(extract_message_meta(routing_context.in_msg)):
            return RoutingDecision(
                routing_context=routing_context,
                strategy=self,
                tier=Tier.TIER_1,
                clickhouse_settings={},
                can_run=True,
            )
        else:
            return RoutingDecision(
                routing_context=routing_context,
                strategy=self,
                tier=Tier.TIER_8,
                clickhouse_settings={},
                can_run=True,
            )


class RoutingStrategyUpdatesQuerySettings(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    def _get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        return RoutingDecision(
            routing_context=routing_context,
            strategy=self,
            tier=Tier.TIER_8,
            clickhouse_settings={"some_setting": "some_value"},
            can_run=True,
        )

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


class RoutingStrategyBadMetrics(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    def _get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        return RoutingDecision(
            routing_context=routing_context,
            strategy=self,
            tier=Tier.TIER_8,
            clickhouse_settings={"some_setting": "some_value"},
            can_run=True,
        )

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        if 1 / 0 > 10:
            return


class RoutingStrategyQueryFails(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    def _get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        return RoutingDecision(
            routing_context=routing_context,
            strategy=self,
            tier=Tier.TIER_8,
            clickhouse_settings={"some_setting": "some_value"},
            can_run=True,
        )

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        raise ValueError("should never get here")


ROUTING_CONTEXT = RoutingContext(
    in_msg=_get_in_msg(),
    timer=Timer("stuff"),
    query_result=MagicMock(spec=QueryResult),
    extra_info={},
)


@pytest.mark.redis_db
def test_target_tier_is_tier_1_if_routing_strategy_fails_to_decide_tier() -> None:
    with mock.patch("snuba.settings.RAISE_ON_ROUTING_STRATEGY_FAILURES", False):
        routing_decision = RoutingStrategyFailsToSelectTier().get_routing_decision(
            deepcopy(ROUTING_CONTEXT)
        )
        assert routing_decision.tier == Tier.TIER_1


@pytest.mark.redis_db
def test_target_tier_is_set_in_routing_context() -> None:
    routing_decision = RoutingStrategySelectsTier8().get_routing_decision(
        deepcopy(ROUTING_CONTEXT)
    )
    assert routing_decision.tier == Tier.TIER_8


@pytest.mark.redis_db
def test_merge_query_settings() -> None:
    routing_decision = RoutingStrategyUpdatesQuerySettings().get_routing_decision(
        deepcopy(ROUTING_CONTEXT)
    )
    assert routing_decision.tier == Tier.TIER_8
    assert routing_decision.clickhouse_settings == {"some_setting": "some_value"}


@pytest.mark.redis_db
def test_routing_strategy_selects_tier_1_if_highest_accuracy_mode() -> None:
    ts = Timestamp()
    ts.GetCurrentTime()
    tstart = Timestamp(seconds=ts.seconds - 3600)
    # don't set downsampled_storage_config
    in_msg = TimeSeriesRequest(
        meta=RequestMeta(
            request_id=RANDOM_REQUEST_ID,
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
    routing_context = deepcopy(ROUTING_CONTEXT)
    routing_context.in_msg = in_msg
    routing_decision = RoutingStrategyHighestAccuracy().get_routing_decision(
        routing_context
    )
    assert routing_decision.tier == Tier.TIER_8

    in_msg.meta.downsampled_storage_config.mode = (
        DownsampledStorageConfig.MODE_HIGHEST_ACCURACY
    )
    routing_decision = RoutingStrategyHighestAccuracy().get_routing_decision(
        routing_context
    )
    assert routing_decision.tier == Tier.TIER_1


@pytest.mark.redis_db
def test_outputting_metrics_fails_open() -> None:
    with mock.patch("snuba.settings.RAISE_ON_ROUTING_STRATEGY_FAILURES", False):
        RoutingStrategyBadMetrics().get_routing_decision(deepcopy(ROUTING_CONTEXT))


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
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

    with mock.patch(
        "snuba.web.rpc.storage_routing.routing_strategies.storage_routing.record_query"
    ) as record_query, mock.patch(
        "snuba.web.rpc.storage_routing.routing_strategy_selector.RoutingStrategySelector.select_routing_strategy",
        return_value=MetricsStrategy(),
    ), mock.patch(
        "snuba.web.rpc.v1.resolvers.R_eap_items.resolver_time_series.run_query",
        return_value=get_query_result(),
    ):
        EndpointTimeSeries().execute(_get_in_msg())
        record_query.assert_called_once()
        recorded_payload = record_query.mock_calls[0].args[0]
        assert recorded_payload["dataset"] == "storage_routing"
        assert recorded_payload["status"] == "TIER_8"
        assert recorded_payload["request"]["referrer"] == "MetricsStrategy"

        assert recorded_payload["query_list"][0]["stats"] == {
            "extra_info": {
                "sampling_in_storage_estimation_time_overhead": {
                    "type": "timing",
                    "value": pytest.approx(
                        0, abs=1
                    ),  # we decide the tier immediately so timing is very small
                    "tags": None,
                },
                "sampling_in_storage_my_metric": {
                    "type": "increment",
                    "value": 1,
                    "tags": {"a": "b", "c": "d"},
                },
                "sampling_in_storage_query_timing": {
                    "type": "timing",
                    "value": 1.0,
                    "tags": {"tier": "TIER_8"},
                },
                "sampling_in_storage_routing_success": {
                    "type": "increment",
                    "value": 1,
                    "tags": {"tier": "TIER_8"},
                },
                "time_budget": 8000,
            },
            "clickhouse_settings": {},
            "source_request_id": RANDOM_REQUEST_ID,
            "result_info": {
                "meta": {},
                "profile": {"bytes": 420, "elapsed": 1.0},
                "sql": "SELECT * FROM your_mom",
                "stats": {"stat": 1},
            },
            "routed_tier": "TIER_8",
            "final": False,
            "cache_hit": 0,
            "can_run": True,
            "max_threads": 0,
            "clickhouse_table": "na",
            "query_id": "na",
            "is_duplicate": 0,
            "consistent": False,
            "strategy": "MetricsStrategy",
        }
        schema = get_codec("snuba-queries")
        payload_bytes = json.dumps(recorded_payload).encode("utf-8")
        schema.decode(payload_bytes)
        assert metric == 1


@pytest.mark.redis_db
def test_get_time_budget() -> None:
    strategy = RoutingStrategySelectsTier8()

    # Test case 1: No config specified - should return default 8000

    assert strategy._get_time_budget_ms() == 8000

    # Test case 2: Global config specified - should return global value
    state.set_config("StorageRouting.time_budget_ms", 5000)
    assert strategy._get_time_budget_ms() == 5000

    # Test case 3: Strategy specific config specified - should return strategy value
    state.set_config("RoutingStrategySelectsTier8.time_budget_ms", 3000)
    assert strategy._get_time_budget_ms() == 3000


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_strategy_exceeds_time_budget() -> None:
    class TooLongStrategy(RoutingStrategySelectsTier8):
        pass

    with mock.patch(
        "snuba.web.rpc.storage_routing.routing_strategies.storage_routing.record_query"
    ) as record_query, mock.patch(
        "snuba.web.rpc.storage_routing.routing_strategy_selector.RoutingStrategySelector.select_routing_strategy",
        return_value=TooLongStrategy(),
    ), mock.patch(
        "snuba.web.rpc.v1.resolvers.R_eap_items.resolver_time_series.run_query",
        return_value=get_query_result(12000),
    ):

        state.set_config("OutcomesBasedRoutingStrategy.time_budget_ms", 8000)
        EndpointTimeSeries().execute(_get_in_msg())
        recorded_payload = record_query.mock_calls[0].args[0]
        assert recorded_payload["query_list"][0]["stats"]["extra_info"][
            "sampling_in_storage_routing_mistake"
        ] == {
            "type": "increment",
            "value": 1,
            "tags": {"reason": "time_budget_exceeded", "tier": "TIER_8"},
        }


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_outcomes_based_routing_metrics_sampled_too_low() -> None:
    class TooFastStrategy(RoutingStrategySelectsTier8):
        pass

    with mock.patch(
        "snuba.web.rpc.storage_routing.routing_strategies.storage_routing.record_query"
    ) as record_query, mock.patch(
        "snuba.web.rpc.storage_routing.routing_strategy_selector.RoutingStrategySelector.select_routing_strategy",
        return_value=TooFastStrategy(),
    ), mock.patch(
        "snuba.web.rpc.v1.resolvers.R_eap_items.resolver_time_series.run_query",
        return_value=get_query_result(900),
    ):

        state.set_config("OutcomesBasedRoutingStrategy.time_budget_ms", 8000)
        EndpointTimeSeries().execute(_get_in_msg())
        recorded_payload = record_query.mock_calls[0].args[0]
        assert recorded_payload["query_list"][0]["stats"]["extra_info"][
            "sampling_in_storage_routing_mistake"
        ] == {
            "type": "increment",
            "value": 1,
            "tags": {"reason": "sampled_too_low", "tier": "TIER_8"},
        }
