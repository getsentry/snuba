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
from snuba.configs.configuration import Configuration, ResourceIdentifier
from snuba.datasets.storages.storage_key import StorageKey
from snuba.downsampled_storage_tiers import Tier
from snuba.query.allocation_policies import (
    MAX_THRESHOLD,
    NO_SUGGESTION,
    NO_UNITS,
    AllocationPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.rpc.common.exceptions import RPCAllocationPolicyException
from snuba.web.rpc.storage_routing.common import extract_message_meta
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
    OutcomesBasedRoutingStrategy,
)
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

    def _update_routing_decision(self, routing_decision: RoutingDecision) -> None:
        raise Exception

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


class RoutingStrategySelectsTier8(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    def _update_routing_decision(self, routing_decision: RoutingDecision) -> None:
        routing_decision.tier = Tier.TIER_8
        routing_decision.can_run = True
        routing_decision.is_throttled = False

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


# a routing strategy that returns tier 1 if the tier is highest accuracy otherwise tier 8
class RoutingStrategyHighestAccuracy(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    def _update_routing_decision(self, routing_decision: RoutingDecision) -> None:
        if self._is_highest_accuracy_mode(
            extract_message_meta(routing_decision.routing_context.in_msg)
        ):
            routing_decision.tier = Tier.TIER_1
        else:
            routing_decision.tier = Tier.TIER_8


class RoutingStrategyUpdatesQuerySettings(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    def _update_routing_decision(self, routing_decision: RoutingDecision) -> None:
        routing_decision.tier = Tier.TIER_8
        routing_decision.clickhouse_settings["some_setting"] = "some_value"

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


class RoutingStrategyBadMetrics(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    def _update_routing_decision(self, routing_decision: RoutingDecision) -> None:
        routing_decision.tier = Tier.TIER_8
        routing_decision.clickhouse_settings["some_setting"] = "some_value"

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        if 1 / 0 > 10:
            return


class RoutingStrategyQueryFails(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    def _update_routing_decision(self, routing_decision: RoutingDecision) -> None:
        routing_decision.tier = Tier.TIER_8
        routing_decision.clickhouse_settings["some_setting"] = "some_value"

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        raise ValueError("should never get here")


class TestRoutingStrategyWithCustomPolicies(OutcomesBasedRoutingStrategy):
    def __init__(self, policies: list[AllocationPolicy]):
        super().__init__()
        self._test_policies = policies

    def get_allocation_policies(self) -> list[AllocationPolicy]:
        return self._test_policies


ROUTING_CONTEXT = RoutingContext(
    in_msg=_get_in_msg(),
    timer=Timer("stuff"),
    query_result=MagicMock(spec=QueryResult),
    extra_info={},
    query_id=uuid.uuid4().hex,
)


@pytest.mark.redis_db
@pytest.mark.eap
def test_target_tier_is_tier_1_if_routing_strategy_fails_to_decide_tier() -> None:
    with mock.patch("snuba.settings.RAISE_ON_ROUTING_STRATEGY_FAILURES", False):
        routing_decision = RoutingStrategyFailsToSelectTier().get_routing_decision(
            deepcopy(ROUTING_CONTEXT)
        )
        assert routing_decision.tier == Tier.TIER_1


@pytest.mark.redis_db
@pytest.mark.eap
def test_target_tier_is_set_in_routing_context() -> None:
    routing_decision = RoutingStrategySelectsTier8().get_routing_decision(deepcopy(ROUTING_CONTEXT))
    assert routing_decision.tier == Tier.TIER_8


@pytest.mark.redis_db
@pytest.mark.eap
def test_merge_query_settings() -> None:
    routing_decision = RoutingStrategyUpdatesQuerySettings().get_routing_decision(
        deepcopy(ROUTING_CONTEXT)
    )
    assert routing_decision.tier == Tier.TIER_8
    assert routing_decision.clickhouse_settings == {"max_threads": 10, "some_setting": "some_value"}


@pytest.mark.redis_db
@pytest.mark.eap
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
    routing_decision = RoutingStrategyHighestAccuracy().get_routing_decision(routing_context)
    assert routing_decision.tier == Tier.TIER_8

    in_msg.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_HIGHEST_ACCURACY
    routing_decision = RoutingStrategyHighestAccuracy().get_routing_decision(routing_context)
    assert routing_decision.tier == Tier.TIER_1


@pytest.mark.redis_db
@pytest.mark.eap
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

    with (
        mock.patch(
            "snuba.web.rpc.storage_routing.routing_strategies.storage_routing.record_query"
        ) as record_query,
        mock.patch(
            "snuba.web.rpc.storage_routing.routing_strategy_selector.RoutingStrategySelector.select_routing_strategy",
            return_value=MetricsStrategy(),
        ),
        mock.patch(
            "snuba.web.rpc.v1.resolvers.R_eap_items.resolver_time_series.run_query",
            return_value=get_query_result(),
        ),
    ):
        EndpointTimeSeries().execute(_get_in_msg())
        record_query.assert_called_once()
        recorded_payload = record_query.mock_calls[0].args[0]
        assert recorded_payload["dataset"] == "storage_routing"
        assert recorded_payload["status"] == "TIER_8"
        assert recorded_payload["request"]["referrer"] == "something"

        # query_id is a uuid, so we don't need to assert it
        recorded_payload["query_list"][0]["stats"].pop("query_id")
        assert recorded_payload["query_list"][0]["stats"] == {
            "extra_info": {
                "sampling_in_storage_estimation_time_overhead": {
                    "type": "timing",
                    "value": pytest.approx(20, abs=10),
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
            "allocation_policies_recommendations": {
                "ConcurrentRateLimitAllocationPolicy": {
                    "can_run": True,
                    "max_threads": 10,
                    "explanation": {},
                    "is_throttled": False,
                    "throttle_threshold": 22,
                    "rejection_threshold": 22,
                    "quota_used": 1,
                    "quota_unit": "concurrent_queries",
                    "suggestion": "no_suggestion",
                    "max_bytes_to_read": 0,
                },
                "ReferrerGuardRailPolicy": {
                    "can_run": True,
                    "max_threads": 10,
                    "explanation": {},
                    "is_throttled": False,
                    "throttle_threshold": 1000000000000,
                    "rejection_threshold": 1000000000000,
                    "quota_used": 0,
                    "quota_unit": "no_units",
                    "suggestion": "no_suggestion",
                    "max_bytes_to_read": 0,
                },
                "BytesScannedRejectingPolicy": {
                    "can_run": True,
                    "max_threads": 10,
                    "explanation": {},
                    "is_throttled": False,
                    "throttle_threshold": 1000000000000,
                    "rejection_threshold": 1000000000000,
                    "quota_used": 0,
                    "quota_unit": "no_units",
                    "suggestion": "no_suggestion",
                    "max_bytes_to_read": 0,
                },
            },
            "clickhouse_settings": {"max_threads": 10},
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
            "is_throttled": False,
            "max_threads": 10,
            "clickhouse_table": "na",
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

    with (
        mock.patch(
            "snuba.web.rpc.storage_routing.routing_strategies.storage_routing.record_query"
        ) as record_query,
        mock.patch(
            "snuba.web.rpc.storage_routing.routing_strategy_selector.RoutingStrategySelector.select_routing_strategy",
            return_value=TooLongStrategy(),
        ),
        mock.patch(
            "snuba.web.rpc.v1.resolvers.R_eap_items.resolver_time_series.run_query",
            return_value=get_query_result(12000),
        ),
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

    with (
        mock.patch(
            "snuba.web.rpc.storage_routing.routing_strategies.storage_routing.record_query"
        ) as record_query,
        mock.patch(
            "snuba.web.rpc.storage_routing.routing_strategy_selector.RoutingStrategySelector.select_routing_strategy",
            return_value=TooFastStrategy(),
        ),
        mock.patch(
            "snuba.web.rpc.v1.resolvers.R_eap_items.resolver_time_series.run_query",
            return_value=get_query_result(900),
        ),
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


@pytest.mark.redis_db
@pytest.mark.eap
def test_routing_strategy_with_rejecting_allocation_policy() -> None:
    update_called = False

    class RejectionPolicy(AllocationPolicy):
        def _additional_config_definitions(self) -> list[Configuration]:
            return []

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            return QuotaAllowance(
                can_run=False,
                max_threads=0,
                explanation={"reason": "policy rejects all queries"},
                is_throttled=True,
                throttle_threshold=MAX_THRESHOLD,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=0,
                quota_unit=NO_UNITS,
                suggestion=NO_SUGGESTION,
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            nonlocal update_called
            update_called = True
            return

    with mock.patch.object(
        BaseRoutingStrategy,
        "get_allocation_policies",
        return_value=[
            RejectionPolicy(ResourceIdentifier(StorageKey("doesntmatter")), ["a", "b", "c"], {})
        ],
    ):
        with pytest.raises(RPCAllocationPolicyException) as excinfo:
            EndpointTimeSeries().execute(_get_in_msg())
        assert update_called
        assert excinfo.value.details["can_run"] == False


@pytest.mark.redis_db
@pytest.mark.eap
def test_routing_strategy_with_throttling_allocation_policy() -> None:
    POLICY_THREADS = 4

    class ThrottleAllocationPolicy(AllocationPolicy):
        def _additional_config_definitions(self) -> list[Configuration]:
            return []

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            return QuotaAllowance(
                can_run=True,
                max_threads=POLICY_THREADS,
                explanation={"reason": "Throttle everything!"},
                is_throttled=True,
                throttle_threshold=MAX_THRESHOLD,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=0,
                quota_unit=NO_UNITS,
                suggestion=NO_SUGGESTION,
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            return

    class ThrottleAllocationPolicyDuplicate(ThrottleAllocationPolicy):
        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            return QuotaAllowance(
                can_run=True,
                max_threads=POLICY_THREADS + 1,
                explanation={"reason": "Throttle everything!"},
                is_throttled=True,
                throttle_threshold=MAX_THRESHOLD,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=0,
                quota_unit=NO_UNITS,
                suggestion=NO_SUGGESTION,
            )

    test_strategy = TestRoutingStrategyWithCustomPolicies(
        policies=[
            ThrottleAllocationPolicy(
                ResourceIdentifier(StorageKey("doesntmatter")), ["a", "b", "c"], {}
            ),
            ThrottleAllocationPolicyDuplicate(
                ResourceIdentifier(StorageKey("doesntmatter")), ["a", "b", "c"], {}
            ),
        ]
    )
    routing_decision = test_strategy.get_routing_decision(deepcopy(ROUTING_CONTEXT))
    assert routing_decision.clickhouse_settings["max_threads"] == POLICY_THREADS
    assert routing_decision.is_throttled == True


@pytest.mark.eap
@pytest.mark.redis_db
def test_allocation_policy_updates_quota() -> None:
    MAX_QUERIES_TO_RUN = 2

    queries_run = 0

    class QueryCountPolicy(AllocationPolicy):
        def _additional_config_definitions(self) -> list[Configuration]:
            return []

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            can_run = True
            suggestion = NO_SUGGESTION
            if queries_run + 1 > MAX_QUERIES_TO_RUN:
                can_run = False
                suggestion = "scan less concurrent queries"
            return QuotaAllowance(
                can_run=can_run,
                max_threads=0,
                explanation={"reason": f"can only run {queries_run} queries!"},
                is_throttled=False,
                throttle_threshold=MAX_QUERIES_TO_RUN,
                rejection_threshold=MAX_QUERIES_TO_RUN,
                quota_used=queries_run + 1,
                quota_unit="queries",
                suggestion=suggestion,
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            nonlocal queries_run
            queries_run += 1

    queries_run_duplicate = 0

    class QueryCountPolicyDuplicate(AllocationPolicy):
        def _additional_config_definitions(self) -> list[Configuration]:
            return []

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            can_run = True
            suggestion = NO_SUGGESTION
            if queries_run_duplicate + 1 > MAX_QUERIES_TO_RUN:
                can_run = False
                suggestion = "scan less concurrent queries"

            return QuotaAllowance(
                can_run=can_run,
                max_threads=0,
                explanation={"reason": f"can only run {queries_run_duplicate} queries!"},
                is_throttled=False,
                throttle_threshold=MAX_QUERIES_TO_RUN,
                rejection_threshold=MAX_QUERIES_TO_RUN,
                quota_used=queries_run + 1,
                quota_unit="queries",
                suggestion=suggestion,
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            nonlocal queries_run_duplicate
            queries_run_duplicate += 1

    # the first policy will error and short circuit the rest
    with mock.patch.object(
        BaseRoutingStrategy,
        "get_allocation_policies",
        return_value=[
            QueryCountPolicy(ResourceIdentifier(StorageKey("doesntmatter")), ["a", "b", "c"], {}),
            QueryCountPolicyDuplicate(
                ResourceIdentifier(StorageKey("doesntmatter")), ["a", "b", "c"], {}
            ),
        ],
    ):
        for _ in range(MAX_QUERIES_TO_RUN):
            EndpointTimeSeries().execute(_get_in_msg())
        with pytest.raises(RPCAllocationPolicyException) as e:
            EndpointTimeSeries().execute(_get_in_msg())

    assert (
        e.value.details["allocation_policies_recommendations"]["QueryCountPolicy"]["can_run"]
        == False
    )
    assert (
        e.value.details["allocation_policies_recommendations"]["QueryCountPolicyDuplicate"][
            "can_run"
        ]
        == False
    )


@pytest.mark.eap
@pytest.mark.redis_db
def test_policy_sets_max_bytes_to_read() -> None:
    class MaximumBytesPolicy(AllocationPolicy):
        def _additional_config_definitions(self) -> list[Configuration]:
            return []

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            return QuotaAllowance(
                can_run=True,
                max_threads=10,
                explanation={},
                is_throttled=True,
                throttle_threshold=420,
                rejection_threshold=42069,
                quota_used=123,
                quota_unit="concurrent_queries",
                suggestion=NO_SUGGESTION,
                max_bytes_to_read=1,
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            pass

    test_strategy = TestRoutingStrategyWithCustomPolicies(
        policies=[
            MaximumBytesPolicy(ResourceIdentifier(StorageKey("doesntmatter")), ["a", "b", "c"], {}),
        ]
    )

    routing_decision = test_strategy.get_routing_decision(deepcopy(ROUTING_CONTEXT))
    assert routing_decision.clickhouse_settings["max_bytes_to_read"] == 1
