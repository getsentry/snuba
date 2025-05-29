import os
import uuid
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    Type,
    TypeAlias,
    Union,
    cast,
    final,
)

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from sentry_kafka_schemas.schema_types import snuba_queries_v1
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba import environment, settings, state
from snuba.attribution import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.downsampled_storage_tiers import Tier
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.state import record_query
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.util import with_span
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc import Tin
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_metadata import (
    RoutingContext,
    RoutingDecision,
)

_SAMPLING_IN_STORAGE_PREFIX = "sampling_in_storage_"
_START_ESTIMATION_MARK = "start_sampling_in_storage_estimation"
_END_ESTIMATION_MARK = "end_sampling_in_storage_estimation"
DEFAULT_STORAGE_ROUTING_CONFIG_PREFIX = "StorageRouting"
MetricsBackendType: TypeAlias = Callable[
    [str, Union[int, float], Optional[Dict[str, str]], Optional[str]], None
]

RoutedRequestType = Union[TimeSeriesRequest, TraceItemTableRequest]
ClickhouseQuerySettings = Dict[str, Any]


def _get_stats_dict(
    routing_decision: RoutingDecision[Tin],
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
    strategy: "BaseRoutingStrategy", routing_decision: RoutingDecision[Tin]
) -> snuba_queries_v1.Querylog:
    cur_span = sentry_sdk.get_current_span()
    assert routing_decision.routing_context is not None
    query_result = routing_decision.routing_context.query_result or QueryResult(
        {}, {"stats": {}, "sql": "", "experiments": {}}
    )
    profile = query_result.result.get("profile", {}) or {}
    return {
        "request": {
            "id": uuid.uuid4().hex,
            "app_id": "storage_routing",
            "body": MessageToDict(routing_decision.routing_context.in_msg),
            "referrer": strategy.__class__.__name__,
        },
        "dataset": "storage_routing",
        "entity": "eap",
        "start_timestamp": routing_decision.routing_context.in_msg.meta.start_timestamp.seconds,  # type: ignore
        "end_timestamp": routing_decision.routing_context.in_msg.meta.end_timestamp.seconds,  # type: ignore
        "status": routing_decision.tier.name,
        "request_status": "NA",
        "slo": "N/A",
        "projects": list(routing_decision.routing_context.in_msg.meta.project_ids)  # type: ignore
        or [],
        "timing": routing_decision.routing_context.timer.for_json(),
        "snql_anonymized": "",
        "query_list": [
            {
                "sql": "",
                "sql_anonymized": "",
                "start_timestamp": routing_decision.routing_context.in_msg.meta.start_timestamp.seconds,  # type: ignore
                "end_timestamp": routing_decision.routing_context.in_msg.meta.end_timestamp.seconds,  # type: ignore
                "stats": _get_stats_dict(routing_decision),
                "status": "0",
                "trace_id": cur_span.trace_id if cur_span else "no_current_span",
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
        "organization": routing_decision.routing_context.in_msg.meta.organization_id,  # type: ignore
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

    def _is_highest_accuracy_mode(self, routing_context: RoutingContext[Tin]) -> bool:
        if not routing_context.in_msg.meta.HasField("downsampled_storage_config"):  # type: ignore
            return False
        return routing_context.in_msg.meta.downsampled_storage_config.mode == DownsampledStorageConfig.MODE_HIGHEST_ACCURACY  # type: ignore

    def _build_snuba_request(
        self, routing_context: RoutingContext[Tin]
    ) -> SnubaRequest:
        request = routing_context.in_msg
        if request.meta.trace_item_type == TraceItemType.TRACE_ITEM_TYPE_LOG:  # type: ignore
            team = "ourlogs"
            feature = "ourlogs"
            parent_api = "ourlog_trace_item_table"
        else:
            team = "eap"
            feature = "eap"
            parent_api = "eap_span_samples"

        return SnubaRequest(
            id=uuid.UUID(request.meta.request_id),  # type: ignore
            original_body=MessageToDict(request),
            query=routing_context.build_query(request),  # type: ignore
            query_settings=routing_context.query_settings,  # type: ignore
            attribution_info=AttributionInfo(
                referrer=request.meta.referrer,  # type: ignore
                team=team,
                feature=feature,
                tenant_ids={
                    "organization_id": request.meta.organization_id,  # type: ignore
                    "referrer": request.meta.referrer,  # type: ignore
                },
                app_id=AppID("eap"),
                parent_api=parent_api,
            ),
        )

    # def _run_query(self, routing_context: RoutingContext[Tin]) -> QueryResult:
    #     snuba_request = self._build_snuba_request(routing_context)
    #     res = run_query(
    #         dataset=PluggableDataset(name="eap", all_entities=[]),
    #         request=snuba_request,
    #         timer=routing_context.timer,
    #     )
    #     routing_context.query_result = res
    #     return res

    def __merge_clickhouse_settings(
        self,
        routing_decision: RoutingDecision[Tin],
        query_settings: ClickhouseQuerySettings,
    ) -> None:
        """merge query settings decided in _decide_tier_and_query_settings with whatever was passed in the
        routing context initially

        the settings in _decide_tier_and_query_settings take priority
        """

        for k, v in query_settings.items():
            routing_decision.clickhouse_settings[k] = v

    def _record_value_in_span_and_DD(
        self,
        routing_context: RoutingContext[Tin],
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

    def _decide_tier_and_query_settings(
        self, routing_decision: RoutingDecision[Tin]
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        raise NotImplementedError

    def __decide_tier_and_query_settings(
        self, timer: Timer, routing_decision: RoutingDecision[Tin]
    ) -> None:
        with sentry_sdk.start_span(op="decide_tier"):
            try:
                timer.mark(_START_ESTIMATION_MARK)
                target_tier, query_settings = self._decide_tier_and_query_settings(
                    routing_decision
                )
                timer.mark(_END_ESTIMATION_MARK)
                self._record_value_in_span_and_DD(
                    routing_decision.routing_context,
                    self.metrics.timing,
                    "estimation_time_overhead",
                    timer.get_duration_between_marks(
                        _START_ESTIMATION_MARK, _END_ESTIMATION_MARK
                    ),
                )
                self.__merge_clickhouse_settings(routing_decision, query_settings)
            except Exception as e:
                # log some error metrics
                self.metrics.increment("estimation_failure")
                sentry_sdk.capture_exception(e)
                target_tier = Tier.TIER_1
                if settings.RAISE_ON_ROUTING_STRATEGY_FAILURES:
                    raise e

            routing_decision.tier = target_tier

    @final
    def __output_metrics(self, routing_decision: RoutingDecision[Tin]) -> None:
        assert routing_decision.routing_context is not None
        self._emit_routing_mistake(routing_decision.routing_context)
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

    def _output_metrics(self, routing_context: RoutingContext[Tin]) -> None:
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

    def _emit_routing_mistake(self, routing_context: RoutingContext[Tin]) -> None:
        if not routing_context.query_result or self._is_highest_accuracy_mode(
            routing_context
        ):
            return
        profile = routing_context.query_result.result.get("profile", {}) or {}
        if elapsed := profile.get("elapsed"):
            elapsed_ms = elapsed * 1000
            time_budget = self._get_time_budget_ms()
            routing_context.extra_info["time_budget"] = time_budget
            if elapsed_ms > time_budget:
                self._record_value_in_span_and_DD(
                    routing_context=routing_context,
                    metrics_backend_func=self.metrics.increment,
                    name="routing_mistake",
                    value=1,
                    tags={
                        "reason": "time_budget_exceeded",
                        "tier": routing_context.query_settings.get_sampling_tier().name,  # type: ignore
                    },
                )
            elif (
                routing_context.query_settings.get_sampling_tier() != Tier.TIER_1  # type: ignore
                and elapsed_ms < self._get_sampled_too_low_threshold()
            ):
                self._record_value_in_span_and_DD(
                    routing_context=routing_context,
                    metrics_backend_func=self.metrics.increment,
                    name="routing_mistake",
                    value=1,
                    tags={
                        "reason": "sampled_too_low",
                        "tier": routing_context.query_settings.get_sampling_tier().name,  # type: ignore
                    },
                )
            else:
                self._record_value_in_span_and_DD(
                    routing_context=routing_context,
                    metrics_backend_func=self.metrics.increment,
                    name="routing_success",
                    value=1,
                    tags={
                        "tier": routing_context.query_settings.get_sampling_tier().name  # type: ignore
                    },
                )

    # @final
    # @with_span(op="function")
    # def run_query_to_correct_tier(self, routing_context: RoutingContext[Tin]) -> QueryResult:
    #     with sentry_sdk.start_span(op="decide_tier"):
    #         try:
    #             routing_context.timer.mark(_START_ESTIMATION_MARK)
    #             target_tier, query_settings = self.decide_tier_and_query_settings(
    #                 routing_context
    #             )
    #             routing_context.timer.mark(_END_ESTIMATION_MARK)
    #             self._record_value_in_span_and_DD(
    #                 routing_context,
    #                 self.metrics.timing,
    #                 "estimation_time_overhead",
    #                 routing_context.timer.get_duration_between_marks(
    #                     _START_ESTIMATION_MARK, _END_ESTIMATION_MARK
    #                 ),
    #             )
    #             self.__merge_clickhouse_settings(routing_context, query_settings)
    #         except Exception as e:
    #             # log some error metrics
    #             self.metrics.increment("estimation_failure")
    #             sentry_sdk.capture_exception(e)
    #             target_tier = Tier.TIER_1
    #             if settings.RAISE_ON_ROUTING_STRATEGY_FAILURES:
    #                 raise e

    #         routing_context.query_settings.set_sampling_tier(target_tier)

    #     with sentry_sdk.start_span(op="run_selected_tier_query") as run_span:
    #         run_span.set_data(
    #             f"{_SAMPLING_IN_STORAGE_PREFIX}.selected_tier",
    #             routing_context.query_settings.get_sampling_tier().name,
    #         )
    #         output = self._run_query(routing_context)
    #         routing_context.query_result = output
    #     with sentry_sdk.start_span(op="output_metrics"):
    #         try:
    #             self.__output_metrics(routing_context)
    #         except Exception as e:
    #             # log some error metrics
    #             self.metrics.increment("metrics_failure")
    #             sentry_sdk.capture_exception(e)
    #             if settings.RAISE_ON_ROUTING_STRATEGY_FAILURES:
    #                 raise e
    #     return routing_context.query_result


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)),
    "snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies",
)
