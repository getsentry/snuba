from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Type, TypeAlias, Union, cast, final

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message as ProtobufMessage
from sentry_kafka_schemas.schema_types import snuba_queries_v1
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import ResponseMeta

from snuba import environment, settings, state
from snuba.downsampled_storage_tiers import Tier
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state import record_query
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory
from snuba.web import QueryResult
from snuba.web.rpc.storage_routing.common import extract_message_meta

_SAMPLING_IN_STORAGE_PREFIX = "sampling_in_storage_"
_START_ESTIMATION_MARK = "start_sampling_in_storage_estimation"
_END_ESTIMATION_MARK = "end_sampling_in_storage_estimation"
DEFAULT_STORAGE_ROUTING_CONFIG_PREFIX = "StorageRouting"
MetricsBackendType: TypeAlias = Callable[
    [str, Union[int, float], Optional[Dict[str, str]], Optional[str]], None
]

RoutedRequestType = Union[TimeSeriesRequest, TraceItemTableRequest]
ClickhouseQuerySettings = Dict[str, Any]


@dataclass
class RoutingContext:
    timer: Timer
    in_msg: ProtobufMessage
    query_result: Optional[QueryResult] = field(default=None)
    extra_info: dict[str, Any] = field(default_factory=dict)


@dataclass
class RoutingDecision:
    routing_context: RoutingContext
    strategy: BaseRoutingStrategy
    tier: Tier = Tier.TIER_1
    clickhouse_settings: dict[str, str] = field(default_factory=dict)
    can_run: bool | None = None

    def to_log_dict(self) -> dict[str, Any]:
        assert self.routing_context is not None
        query_result: dict[str, Any] = {}
        if self.routing_context.query_result:
            query_result["meta"] = self.routing_context.query_result.result.get(
                "meta", {}
            )
            query_result["profile"] = self.routing_context.query_result.result.get(
                "profile", {}
            )
            query_result["stats"] = self.routing_context.query_result.extra.get("stats")
            query_result["sql"] = self.routing_context.query_result.extra.get("sql")

        in_msg_meta = extract_message_meta(self.routing_context.in_msg)

        return {
            "can_run": self.can_run,
            "strategy": self.strategy.__class__.__name__,
            "source_request_id": in_msg_meta.request_id,
            "extra_info": self.routing_context.extra_info,
            "clickhouse_settings": self.clickhouse_settings,
            "result_info": query_result,
            "routed_tier": self.tier.name,
        }


def _get_stats_dict(
    routing_decision: RoutingDecision,
) -> snuba_queries_v1._QueryMetadataStats:
    return cast(
        snuba_queries_v1._QueryMetadataStats,
        {
            "final": False,
            "cache_hit": 0,
            "max_threads": routing_decision.clickhouse_settings.get("max_threads", 0),
            "clickhouse_table": "na",
            "query_id": "na",
            "is_duplicate": 0,
            "consistent": False,
            **routing_decision.to_log_dict(),
        },
    )


def _construct_hacky_querylog_payload(
    strategy: "BaseRoutingStrategy", routing_decision: RoutingDecision
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
            "id": uuid.uuid4().hex,
            "app_id": "storage_routing",
            "body": MessageToDict(routing_decision.routing_context.in_msg)
            if routing_decision.routing_context.in_msg
            else {},
            "referrer": strategy.__class__.__name__,
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
                "stats": _get_stats_dict(routing_decision),
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


class BaseRoutingStrategy(metaclass=RegisteredClass):
    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @property
    def metrics(self) -> MetricsWrapper:
        return MetricsWrapper(
            environment.metrics,
            "routing_strategy",
            tags={"routing_strategy_name": self.__class__.__name__},
        )

    @classmethod
    def get_from_name(cls, name: str) -> Type["BaseRoutingStrategy"]:
        return cast("Type[BaseRoutingStrategy]", cls.class_from_name(name))

    def _is_highest_accuracy_mode(self, in_msg_meta: ResponseMeta) -> bool:
        if not hasattr(in_msg_meta, "downsampled_storage_config"):
            return False
        return bool(
            in_msg_meta.downsampled_storage_config.mode
            == DownsampledStorageConfig.MODE_HIGHEST_ACCURACY
        )

    def merge_clickhouse_settings(
        self,
        routing_decision: RoutingDecision,
        query_settings: HTTPQuerySettings,
    ) -> None:
        """merge query settings decided in _get_routing_decision with whatever was passed in (from the request) initially

        the settings in _get_routing_decision take priority

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
        tags: Dict[str, str] | None = None,
    ) -> None:
        name = _SAMPLING_IN_STORAGE_PREFIX + name
        metrics_backend_func(name, value, tags, None)
        span = sentry_sdk.get_current_span()
        routing_context.extra_info[name] = {
            "type": metrics_backend_func.__name__,
            "value": value,
            "tags": tags,
        }
        if span is not None:
            span.set_data(name, value)

    def _get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        raise NotImplementedError

    @final
    def get_routing_decision(self, routing_context: RoutingContext) -> RoutingDecision:
        from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
            OutcomesBasedRoutingStrategy,
        )

        with sentry_sdk.start_span(op="decide_tier"):
            try:
                routing_context.timer.mark(_START_ESTIMATION_MARK)
                routing_decision = self._get_routing_decision(routing_context)
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
                    tier=Tier.TIER_1,
                    can_run=True,
                )

                if settings.RAISE_ON_ROUTING_STRATEGY_FAILURES:
                    raise e

            return routing_decision

    @final
    def output_metrics(self, routing_decision: RoutingDecision) -> None:
        assert routing_decision.routing_context is not None
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

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass

    def _get_sampled_too_low_threshold(self) -> int:
        default = 1000
        return (
            state.get_int_config(
                f"{self.config_key()}.sampled_too_low_threshold",
                state.get_int_config(
                    f"{DEFAULT_STORAGE_ROUTING_CONFIG_PREFIX}.sampled_too_low_threshold",
                    default,
                )
                or default,
            )
            or default
        )

    def _get_time_budget_ms(self) -> int:
        """
        Get the time budget for the query, Each strategy can have its own
        time budget overridden or can default to a global one set in runtime config
        """
        default = 8000
        return (
            state.get_int_config(
                f"{self.config_key()}.time_budget_ms",
                state.get_int_config(
                    f"{DEFAULT_STORAGE_ROUTING_CONFIG_PREFIX}.time_budget_ms",
                    default,
                )
                or default,
            )
            or default
        )

    def _emit_routing_mistake(self, routing_decision: RoutingDecision) -> None:
        if routing_decision.routing_context.query_result is None:
            return
        if self._is_highest_accuracy_mode(
            extract_message_meta(routing_decision.routing_context.in_msg)
        ):
            return
        profile = (
            routing_decision.routing_context.query_result.result.get("profile", {})
            or {}
        )
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


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)),
    "snuba.web.rpc.storage_routing.routing_strategies",
)
