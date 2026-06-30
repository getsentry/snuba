from __future__ import annotations

import os
from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC
from typing import (
    Any,
    NamedTuple,
    TypedDict,
    cast,
    final,
)

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message as ProtobufMessage
from google.protobuf.timestamp_pb2 import Timestamp as TimestampProto
from sentry_kafka_schemas.schema_types import snuba_queries_v1
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta

from snuba import environment, settings
from snuba.configs.configuration import (
    ConfigurableComponent,
    ConfigurableComponentData,
    Configuration,
    ResourceIdentifier,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.downsampled_storage_tiers import Tier
from snuba.query.allocation_policies import (
    AllocationPolicy,
    PolicyData,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.query.allocation_policies.bytes_scanned_rejecting_policy import (
    BytesScannedRejectingPolicy,
)
from snuba.query.allocation_policies.concurrent_rate_limit import (
    ConcurrentRateLimitAllocationPolicy,
)
from snuba.query.allocation_policies.per_referrer import ReferrerGuardRailPolicy
from snuba.query.allocation_policies.utils import get_max_bytes_to_read
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state import record_query
from snuba.state.sentry_options import get_mapped_option, get_option
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.registered_class import import_submodules_in_directory
from snuba.web import QueryException, QueryResult
from snuba.web.rpc.common.exceptions import RPCAllocationPolicyException
from snuba.web.rpc.storage_routing.common import extract_message_meta
from snuba.web.rpc.storage_routing.load_retriever import LoadInfo, get_cluster_loadinfo

_SAMPLING_IN_STORAGE_PREFIX = "sampling_in_storage_"
_START_ESTIMATION_MARK = "start_sampling_in_storage_estimation"
_END_ESTIMATION_MARK = "end_sampling_in_storage_estimation"
DEFAULT_STORAGE_ROUTING_CONFIG_PREFIX = "StorageRouting"
MetricsBackendType = Callable[[str, int | float, dict[str, str] | None, str | None], None]
CBRS_HASH = "cbrs"
RoutedRequestType = TimeSeriesRequest | TraceItemTableRequest
ClickhouseQuerySettings = dict[str, Any]


class _OrgOverridableSetting(NamedTuple):
    """One entry in the per-org ClickHouse setting override allowlist.

    `unset_sentinel` is the value stored when no override is configured — chosen
    so it cannot collide with a legitimate value an operator might want to set
    (e.g. ClickHouse treats max_threads=0 as 'use all available cores', so 0 is
    a real value and -1 is the sentinel). `description` is the operator-facing
    string shown in the admin UI for this setting's override.
    """

    value_type: type
    unset_sentinel: Any
    description: str


# Allowlist of ClickHouse settings that operators may override per-organization
# via routing-strategy config. Each entry adds an `organization_<name>_override`
# Configuration to every routing strategy, keyed by organization_id. The override
# is applied after _update_routing_decision and replaces any prior value.
ORG_OVERRIDABLE_CLICKHOUSE_SETTINGS: dict[str, _OrgOverridableSetting] = {
    "max_threads": _OrgOverridableSetting(
        value_type=int,
        unset_sentinel=-1,
        description=(
            "Per-organization_id override for the ClickHouse max_threads setting. "
            "Replaces any value set by allocation policies or the routing strategy, "
            "including raising it above the policy-derived value. Default -1 means "
            "no override; note that 0 is a legitimate value (ClickHouse interprets "
            "max_threads=0 as 'use all available physical cores')."
        ),
    ),
}


@dataclass
class RoutingContext:
    timer: Timer
    in_msg: ProtobufMessage
    query_id: str
    query_result: QueryResult | None = field(default=None)
    extra_info: dict[str, Any] = field(default_factory=dict)
    allocation_policies_recommendations: dict[str, QuotaAllowance] = field(default_factory=dict)
    cluster_load_info: LoadInfo | None = field(default=None)

    @property
    def tenant_ids(self) -> dict[str, str | int]:
        request_meta = extract_message_meta(self.in_msg)
        return {
            "organization_id": request_meta.organization_id,
            "referrer": request_meta.referrer,
            **(
                {"project_id": request_meta.project_ids[0]}
                if hasattr(request_meta, "project_ids") and len(request_meta.project_ids) == 1
                else {}
            ),
        }


@dataclass
class TimeWindow:
    start_timestamp: TimestampProto
    end_timestamp: TimestampProto

    def length_hours(self) -> float:
        return (self.end_timestamp.seconds - self.start_timestamp.seconds) / 3600

    def __repr__(self) -> str:
        from datetime import datetime

        start = datetime.fromtimestamp(self.start_timestamp.seconds, tz=UTC).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end = datetime.fromtimestamp(self.end_timestamp.seconds, tz=UTC).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        return f"TimeWindow(start={start}, end={end})"


@dataclass
class RoutingDecision:
    routing_context: RoutingContext
    strategy: BaseRoutingStrategy
    tier: Tier = Tier.TIER_1
    clickhouse_settings: dict[str, Any] = field(default_factory=dict)
    can_run: bool | None = None
    time_window: TimeWindow | None = None
    is_throttled: bool | None = None

    def to_log_dict(self) -> dict[str, Any]:
        assert self.routing_context is not None
        query_result: dict[str, Any] = {}
        if self.routing_context.query_result:
            query_result["meta"] = self.routing_context.query_result.result.get("meta", {})
            query_result["profile"] = self.routing_context.query_result.result.get("profile", {})
            query_result["stats"] = self.routing_context.query_result.extra.get("stats")
            query_result["sql"] = self.routing_context.query_result.extra.get("sql")

        in_msg_meta = extract_message_meta(self.routing_context.in_msg)

        return {
            "can_run": self.can_run,
            "is_throttled": self.is_throttled,
            "strategy": self.strategy.__class__.__name__,
            "source_request_id": in_msg_meta.request_id,
            "extra_info": self.routing_context.extra_info,
            "clickhouse_settings": self.clickhouse_settings,
            "result_info": query_result,
            "routed_tier": self.tier.name,
            "allocation_policies_recommendations": {
                key: quota_allowance.to_dict()
                for key, quota_allowance in self.routing_context.allocation_policies_recommendations.items()
            },
        }


class CombinedAllocationPoliciesRecommendations(TypedDict):
    can_run: bool
    is_throttled: bool
    settings: dict[str, Any]


def get_stats_dict(
    routing_decision: RoutingDecision,
) -> snuba_queries_v1._QueryMetadataStats:
    return cast(
        snuba_queries_v1._QueryMetadataStats,
        {
            "final": False,
            "cache_hit": 0,
            "max_threads": routing_decision.clickhouse_settings.get("max_threads", 0),
            "clickhouse_table": "na",
            "query_id": routing_decision.routing_context.query_id,
            "is_duplicate": 0,
            "consistent": False,
            **routing_decision.to_log_dict(),
        },
    )


def _construct_hacky_querylog_payload(
    strategy: BaseRoutingStrategy, routing_decision: RoutingDecision
) -> snuba_queries_v1.Querylog:
    cur_span = sentry_sdk.get_current_span()
    assert routing_decision.routing_context is not None
    query_result = routing_decision.routing_context.query_result or QueryResult(
        {}, {"stats": {}, "sql": "", "experiments": {}}
    )
    profile = query_result.result.get("profile", {}) or {}
    in_message_meta = extract_message_meta(routing_decision.routing_context.in_msg)

    return {
        "request": {
            "id": routing_decision.routing_context.query_id,
            "app_id": "storage_routing",
            "body": (
                MessageToDict(routing_decision.routing_context.in_msg)
                if routing_decision.routing_context.in_msg
                else {}
            ),
            "referrer": cast(str, routing_decision.routing_context.tenant_ids["referrer"]),
        },
        "dataset": "storage_routing",
        "entity": "eap",
        "start_timestamp": in_message_meta.start_timestamp.seconds,
        "end_timestamp": in_message_meta.end_timestamp.seconds,
        "status": routing_decision.tier.name,
        "request_status": "NA",
        "slo": "N/A",
        "projects": list(in_message_meta.project_ids) or [],
        "timing": routing_decision.routing_context.timer.for_json(),
        "snql_anonymized": "",
        "query_list": [
            {
                "sql": "",
                "sql_anonymized": "",
                "start_timestamp": in_message_meta.start_timestamp.seconds,
                "end_timestamp": in_message_meta.end_timestamp.seconds,
                "stats": get_stats_dict(routing_decision),
                "status": "0",
                "trace_id": cur_span.trace_id if cur_span else "",
                "profile": {
                    "time_range": None,
                    "table": "eap_items",
                    "all_columns": [],
                    "multi_level_condition": False,
                    "where_profile": {"columns": [], "mapping_cols": []},
                    "array_join_cols": [],
                    "groupby_cols": [],
                },
                "result_profile": {
                    "bytes": cast(int, profile.get("bytes", 0)),
                    "progress_bytes": cast(int, profile.get("progress_bytes", 0)),
                    "elapsed": cast(float, profile.get("elapsed", 0)),
                },
                "request_status": "na",
                "slo": "na",
            }
        ],
        "organization": in_message_meta.organization_id,
    }


@dataclass()
class RoutingStrategyConfig(Configuration):
    pass


class StrategyData(ConfigurableComponentData):
    policies_data: list[PolicyData]


class BaseRoutingStrategy(ConfigurableComponent, ABC):
    def __init__(self, default_config_overrides: dict[str, Any] | None = None) -> None:
        if default_config_overrides is None:
            default_config_overrides = {}
        self._default_config_definitions = [
            RoutingStrategyConfig(
                name="some_default_config",
                description="Placeholder for now",
                value_type=int,
                default=100,
            ),
        ]
        for setting_name, setting in ORG_OVERRIDABLE_CLICKHOUSE_SETTINGS.items():
            self._default_config_definitions.append(
                RoutingStrategyConfig(
                    name=f"organization_{setting_name}_override",
                    description=setting.description,
                    value_type=setting.value_type,
                    default=setting.unset_sentinel,
                    param_types={"organization_id": int},
                )
            )
        self._overridden_additional_config_definitions = (
            self._get_overridden_additional_config_defaults(default_config_overrides)
        )

    def _get_hash(self) -> str:
        return CBRS_HASH

    def _get_default_config_definitions(self) -> list[Configuration]:
        return cast(list[Configuration], self._default_config_definitions)

    def _get_default_routing_decision_tier(self) -> Tier:
        tier_int = get_option("default_tier", 1)

        if tier_int == 512:
            return Tier.TIER_512
        if tier_int == 64:
            return Tier.TIER_64
        if tier_int == 8:
            return Tier.TIER_8
        return Tier.TIER_1

    def additional_config_definitions(self) -> list[Configuration]:
        return self._overridden_additional_config_definitions

    @property
    def resource_identifier(self) -> ResourceIdentifier:
        return ResourceIdentifier(
            StorageKey.EAP_ITEMS,
        )

    @property
    def metrics(self) -> MetricsWrapper:
        return MetricsWrapper(
            environment.metrics,
            "routing_strategy",
            tags={"routing_strategy_name": self.__class__.__name__},
        )

    @classmethod
    def create_minimal_instance(cls, resource_identifier: str) -> ConfigurableComponent:
        return cls(
            default_config_overrides={},
        )

    def _is_highest_accuracy_mode(self, in_msg_meta: RequestMeta) -> bool:
        if in_msg_meta.HasField("downsampled_storage_config"):
            return bool(
                in_msg_meta.downsampled_storage_config.mode
                == DownsampledStorageConfig.MODE_HIGHEST_ACCURACY
            )
        return False

    def get_allocation_policies(self) -> list[AllocationPolicy]:
        # by default all routing strategies share allocation policies since effectively they are all
        # protecting the same resource (EAP)
        EAP_RESOURCE_IDENTIFIER = ResourceIdentifier("EAP")
        return [
            ConcurrentRateLimitAllocationPolicy(
                storage_key=EAP_RESOURCE_IDENTIFIER,
                required_tenant_types=["organization_id", "referrer", "project_id"],
                default_config_overrides={"is_enforced": 0, "concurrent_limit": 66},
            ),
            ReferrerGuardRailPolicy(
                storage_key=EAP_RESOURCE_IDENTIFIER,
                required_tenant_types=["referrer"],
                default_config_overrides={"is_enforced": 0, "is_active": 0},
            ),
            BytesScannedRejectingPolicy(
                storage_key=EAP_RESOURCE_IDENTIFIER,
                required_tenant_types=["organization_id", "project_id", "referrer"],
                default_config_overrides={"is_active": 0, "is_enforced": 0},
            ),
        ]

    def get_delete_allocation_policies(self) -> list[AllocationPolicy]:
        return []

    def merge_clickhouse_settings(
        self,
        routing_decision: RoutingDecision,
        query_settings: HTTPQuerySettings,
    ) -> None:
        """merge query settings decided in _update_routing_decision with whatever was passed in (from the request) initially

        the settings in _update_routing_decision take priority

        note that for querylog, routing_decision.clickhouse_settings will not contain whatever was passed in
        """

        for k, v in routing_decision.clickhouse_settings.items():
            query_settings.push_clickhouse_setting(k, v)

    def _record_value_in_span_and_DD(
        self,
        routing_context: RoutingContext,
        metrics_backend_func: MetricsBackendType,
        name: str,
        value: float | int,
        tags: dict[str, str] | None = None,
    ) -> None:
        name = _SAMPLING_IN_STORAGE_PREFIX + name
        metrics_backend_func(name, value, tags, None)
        routing_context.extra_info[name] = {
            "type": metrics_backend_func.__name__,
            "value": value,
            "tags": tags,
        }
        sentry_sdk.update_current_span(attributes={name: value})

    def _update_routing_decision(
        self,
        routing_decision: RoutingDecision,
    ) -> None:
        raise NotImplementedError

    def _get_org_clickhouse_setting_overrides(
        self, tenant_ids: dict[str, str | int]
    ) -> dict[str, Any]:
        org_id = tenant_ids.get("organization_id")
        if org_id is None:
            return {}
        overrides: dict[str, Any] = {}
        for setting_name, setting in ORG_OVERRIDABLE_CLICKHOUSE_SETTINGS.items():
            value = self.get_config_value(
                f"organization_{setting_name}_override",
                {"organization_id": org_id},
            )
            if value != setting.unset_sentinel:
                overrides[setting_name] = value
        return overrides

    def _get_combined_allocation_policies_recommendations(
        self, policy_recommendations: list[QuotaAllowance]
    ) -> CombinedAllocationPoliciesRecommendations:
        # decides how to combine the recommendations from the allocation policies
        settings = {}

        max_bytes_to_read = get_max_bytes_to_read(policy_recommendations)
        if max_bytes_to_read != 0:
            settings["max_bytes_to_read"] = max_bytes_to_read

        settings["max_threads"] = min(
            [qa.max_threads for qa in policy_recommendations],
        )

        return CombinedAllocationPoliciesRecommendations(
            can_run=all(qa.can_run for qa in policy_recommendations),
            is_throttled=any(qa.is_throttled for qa in policy_recommendations),
            settings=settings,
        )

    def _get_recommendations_from_allocation_policies(
        self,
        routing_context: RoutingContext,
    ) -> dict[str, QuotaAllowance]:
        recommendations: dict[str, QuotaAllowance] = {}
        for allocation_policy in self.get_allocation_policies():
            allocation_policy_name = allocation_policy.class_name()
            with sentry_sdk.start_span(
                op="allocation_policy.get_quota_allowance",
                description=allocation_policy_name,
            ) as span:
                recommendations[allocation_policy_name] = allocation_policy.get_quota_allowance(
                    routing_context.tenant_ids,
                    routing_context.query_id,
                )
                span.set_data(
                    f"{allocation_policy_name}_quota_allowance",
                    recommendations[allocation_policy_name],
                )
        return recommendations

    @final
    def get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
            OutcomesBasedRoutingStrategy,
        )

        default_tier = self._get_default_routing_decision_tier()

        with sentry_sdk.start_span(op="decide_tier") as span:
            try:
                routing_context.timer.mark(_START_ESTIMATION_MARK)

                routing_context.allocation_policies_recommendations = (
                    self._get_recommendations_from_allocation_policies(routing_context)
                )
                combined_allocation_policies_recommendations = (
                    self._get_combined_allocation_policies_recommendations(
                        list(routing_context.allocation_policies_recommendations.values())
                    )
                )

                routing_decision = RoutingDecision(
                    routing_context=routing_context,
                    strategy=self,
                    tier=default_tier,
                    clickhouse_settings=combined_allocation_policies_recommendations["settings"],
                    can_run=combined_allocation_policies_recommendations["can_run"],
                    is_throttled=combined_allocation_policies_recommendations["is_throttled"],
                )

                routing_context.cluster_load_info = (
                    get_cluster_loadinfo()
                    if get_option("storage_routing.enable_get_cluster_loadinfo", False)
                    else None
                )

                self._update_routing_decision(routing_decision)

                org_overrides = self._get_org_clickhouse_setting_overrides(
                    routing_context.tenant_ids
                )
                if org_overrides:
                    routing_decision.clickhouse_settings.update(org_overrides)
                    routing_context.extra_info["org_clickhouse_setting_overrides"] = org_overrides

                routing_context.timer.mark(_END_ESTIMATION_MARK)
                self._record_value_in_span_and_DD(
                    routing_decision.routing_context,
                    self.metrics.timing,
                    "estimation_time_overhead",
                    routing_context.timer.get_duration_between_marks(
                        _START_ESTIMATION_MARK, _END_ESTIMATION_MARK
                    ),
                )

            except Exception as e:
                # log some error metrics
                self.metrics.increment("estimation_failure")
                sentry_sdk.capture_message(f"Error getting routing decision: {e}")
                routing_decision = RoutingDecision(
                    routing_context=routing_context,
                    strategy=OutcomesBasedRoutingStrategy(),
                    tier=default_tier,
                    can_run=True,
                )

                if settings.RAISE_ON_ROUTING_STRATEGY_FAILURES:
                    raise e
            span.set_data("decided_tier", routing_decision.tier)
            return routing_decision

    @final
    def after_execute(self, routing_decision: RoutingDecision, error: Exception | None) -> None:
        try:
            assert routing_decision.routing_context is not None

            self.update_allocation_policies_balances(routing_decision, error)

            # these metrics are meant to track reject/throttle/success decisions, so they get emitted even if the query did not run successfully after routing
            tags = {
                "strategy": self.class_name(),
                "resource_identifier": routing_decision.strategy.resource_identifier.value,
                "referrer": cast(str, routing_decision.routing_context.tenant_ids["referrer"]),
            }
            if not routing_decision.can_run:
                self.metrics.increment("rejected_query", tags=tags)
            elif routing_decision.is_throttled:
                self.metrics.increment("throttled_query", tags=tags)
            else:
                self.metrics.increment("successful_query", tags=tags)

            self._emit_routing_mistake(routing_decision)
            self._output_metrics(routing_decision.routing_context)
            query_result = routing_decision.routing_context.query_result or QueryResult(
                {}, {"stats": {}, "sql": "", "experiments": {}}
            )
            profile = query_result.result.get("profile", {}) or {}
            if elapsed := profile.get("elapsed"):
                self._record_value_in_span_and_DD(
                    routing_context=routing_decision.routing_context,
                    metrics_backend_func=self.metrics.timing,
                    name="query_timing",
                    value=elapsed,
                    tags={"tier": routing_decision.tier.name},
                )
            if bytes_scanned := profile.get("progress_bytes"):
                self._record_value_in_span_and_DD(
                    routing_context=routing_decision.routing_context,
                    metrics_backend_func=self.metrics.timing,
                    name="query_bytes_scanned",
                    value=bytes_scanned,
                    tags={"tier": routing_decision.tier.name},
                )
            record_query(_construct_hacky_querylog_payload(self, routing_decision))
        except Exception as e:
            self.metrics.increment("after_execute_failure")
            sentry_sdk.capture_message(f"Error in routing strategy after execute: {e}")
            if settings.RAISE_ON_ROUTING_STRATEGY_FAILURES:
                raise e

    @final
    def update_allocation_policies_balances(
        self, routing_decision: RoutingDecision, error: Exception | None
    ) -> None:
        if routing_decision.routing_context.query_result is not None or isinstance(
            error, (QueryException, RPCAllocationPolicyException)
        ):
            query_result_or_error = QueryResultOrError(
                query_result=routing_decision.routing_context.query_result, error=error
            )
            for allocation_policy in self.get_allocation_policies():
                allocation_policy.update_quota_balance(
                    tenant_ids=routing_decision.routing_context.tenant_ids,
                    query_id=routing_decision.routing_context.query_id,
                    result_or_error=query_result_or_error,
                )

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass

    def _get_sampled_too_low_threshold(self) -> int:
        # Per-strategy override, falling back to the global "StorageRouting"
        # default, then the constant. The dict is keyed by routing-strategy class
        # name (or DEFAULT_STORAGE_ROUTING_CONFIG_PREFIX for the global value).
        default = 1000
        return cast(
            int,
            get_mapped_option(
                "storage_routing_sampled_too_low_threshold",
                self.class_name(),
                get_mapped_option(
                    "storage_routing_sampled_too_low_threshold",
                    DEFAULT_STORAGE_ROUTING_CONFIG_PREFIX,
                    default,
                )
                or default,
            )
            or default,
        )

    def _get_time_budget_ms(self) -> int:
        """
        Get the time budget for the query, Each strategy can have its own
        time budget overridden or can default to a global one set in runtime config
        """
        default = 8000
        return cast(
            int,
            get_mapped_option(
                "storage_routing_time_budget_ms",
                self.class_name(),
                get_mapped_option(
                    "storage_routing_time_budget_ms",
                    DEFAULT_STORAGE_ROUTING_CONFIG_PREFIX,
                    default,
                )
                or default,
            )
            or default,
        )

    def _emit_routing_mistake(self, routing_decision: RoutingDecision) -> None:
        if routing_decision.routing_context.query_result is None:
            return
        if self._is_highest_accuracy_mode(
            extract_message_meta(routing_decision.routing_context.in_msg)
        ):
            return
        profile = routing_decision.routing_context.query_result.result.get("profile", {}) or {}
        if elapsed := profile.get("elapsed"):
            elapsed_ms = elapsed * 1000
            time_budget = self._get_time_budget_ms()
            routing_decision.routing_context.extra_info["time_budget"] = time_budget
            if elapsed_ms > time_budget:
                self._record_value_in_span_and_DD(
                    routing_context=routing_decision.routing_context,
                    metrics_backend_func=self.metrics.increment,
                    name="routing_mistake",
                    value=1,
                    tags={
                        "reason": "time_budget_exceeded",
                        "tier": routing_decision.tier.name,
                    },
                )
            elif (
                routing_decision.tier != Tier.TIER_1
                and elapsed_ms < self._get_sampled_too_low_threshold()
            ):
                self._record_value_in_span_and_DD(
                    routing_context=routing_decision.routing_context,
                    metrics_backend_func=self.metrics.increment,
                    name="routing_mistake",
                    value=1,
                    tags={
                        "reason": "sampled_too_low",
                        "tier": routing_decision.tier.name,
                    },
                )
            else:
                self._record_value_in_span_and_DD(
                    routing_context=routing_decision.routing_context,
                    metrics_backend_func=self.metrics.increment,
                    name="routing_success",
                    value=1,
                    tags={
                        "tier": routing_decision.tier.name,
                    },
                )

    def to_dict(self) -> StrategyData:
        base_data = super().to_dict()
        policies = self.get_allocation_policies() + self.get_delete_allocation_policies()
        return StrategyData(**base_data, policies_data=[policy.to_dict() for policy in policies])


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)),
    "snuba.web.rpc.storage_routing.routing_strategies",
)
