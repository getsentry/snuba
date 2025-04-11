import uuid
from dataclasses import asdict
from typing import Any, Callable, Dict, Optional, Tuple, TypeAlias, TypeVar, Union, cast

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_sdk.tracing import Span

from snuba import state
from snuba.attribution import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.downsampled_storage_tiers import Tier
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.request import Request
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.util import with_span
from snuba.web import QueryResult
from snuba.web.query import run_query

T = TypeVar("T", TimeSeriesRequest, TraceItemTableRequest)
MetricsBackendType: TypeAlias = Callable[
    [str, Union[int, float], Optional[Dict[str, str]], Optional[str]], None
]


DOWNSAMPLING_TIER_MULTIPLIERS: Dict[Tier, int] = {
    Tier.TIER_64: 8,
    Tier.TIER_8: 64,
    Tier.TIER_1: 512,
}
_SAMPLING_IN_STORAGE_PREFIX = "sampling_in_storage_"
_START_ESTIMATION_MARK = "start_sampling_in_storage_estimation"
_END_ESTIMATION_MARK = "end_sampling_in_storage_estimation"


def _get_time_budget() -> float:
    sentry_timeout_ms = cast(
        int,
        state.get_int_config(
            _SAMPLING_IN_STORAGE_PREFIX + "sentry_timeout", default=30000
        ),
    )  # 30s = 30000ms
    error_budget_ms = cast(
        int,
        state.get_int_config(
            _SAMPLING_IN_STORAGE_PREFIX + "error_budget", default=5000
        ),
    )  # 5s = 5000ms
    return sentry_timeout_ms - error_budget_ms


def _get_bytes_scanned_limit() -> int:
    return cast(
        int,
        state.get_int_config(
            _SAMPLING_IN_STORAGE_PREFIX + "bytes_scanned_per_query_limit",
            default=161061273600,
        ),
    )  # 150 gigabytes is default


def _exclude_user_data_from_res(res: QueryResult) -> Dict[str, Any]:
    result_dict = asdict(res)
    result_dict["result"] = {
        k: v for k, v in result_dict["result"].items() if k != "data"
    }
    return result_dict


def _get_query_bytes_scanned(res: QueryResult, span: Span) -> int:
    profile_dict = res.result.get("profile") or {}
    progress_bytes = profile_dict.get("progress_bytes")

    if progress_bytes is not None:
        return cast(int, progress_bytes)

    error_type = (
        "QueryResult_contains_no_profile"
        if profile_dict is None
        else "QueryResult_contains_no_progress_bytes"
    )
    res_without_user_data = _exclude_user_data_from_res(res)
    sentry_sdk.capture_message(f"{error_type}: {res_without_user_data}", level="error")
    span.set_data(error_type, res_without_user_data)
    return 0


def _get_query_duration_ms(res: QueryResult) -> float:
    return cast(float, res.result.get("profile", {}).get("elapsed", 0) * 1000)  # type: ignore


def _get_most_downsampled_tier() -> Tier:
    return sorted(Tier, reverse=True)[0]


def _record_value_in_span_and_DD(
    span: Span,
    metrics_backend_func: MetricsBackendType,
    name: str,
    value: float | int,
    tags: Dict[str, str],
) -> None:
    name = _SAMPLING_IN_STORAGE_PREFIX + name
    metrics_backend_func(name, value, tags, None)
    span.set_data(name, value)


def _get_target_tier(
    most_downsampled_res: QueryResult,
    metrics_backend: MetricsBackend,
    referrer: str,
    timer: Timer,
) -> Tuple[Tier, int]:
    with sentry_sdk.start_span(op="_get_target_tier") as span:
        most_downsampled_query_bytes_scanned = _get_query_bytes_scanned(
            most_downsampled_res, span
        )

        span.set_data(
            _SAMPLING_IN_STORAGE_PREFIX + "most_downsampled_query_bytes_scanned",
            most_downsampled_query_bytes_scanned,
        )

        target_tier = _get_most_downsampled_tier()
        estimated_target_tier_bytes_scanned = (
            most_downsampled_query_bytes_scanned
            * cast(int, DOWNSAMPLING_TIER_MULTIPLIERS.get(target_tier))
        )
        for tier in sorted(Tier, reverse=True)[:-1]:
            with sentry_sdk.start_span(
                op=f"_get_target_tier.Tier_{tier}"
            ) as tier_specific_span:
                estimated_query_bytes_scanned_to_this_tier = (
                    most_downsampled_query_bytes_scanned
                    * cast(int, DOWNSAMPLING_TIER_MULTIPLIERS.get(tier))
                )

                _record_value_in_span_and_DD(
                    tier_specific_span,
                    metrics_backend.distribution,
                    "estimated_query_bytes_scanned",
                    estimated_query_bytes_scanned_to_this_tier,
                    {"referrer": referrer, "tier": str(tier)},
                )

                bytes_scanned_limit = _get_bytes_scanned_limit()
                if estimated_query_bytes_scanned_to_this_tier <= bytes_scanned_limit:
                    target_tier = tier
                    estimated_target_tier_bytes_scanned = (
                        estimated_query_bytes_scanned_to_this_tier
                    )

                tier_specific_span.set_data(
                    _SAMPLING_IN_STORAGE_PREFIX + "target_tier", target_tier
                )
                tier_specific_span.set_data(
                    _SAMPLING_IN_STORAGE_PREFIX + "bytes_scanned_limit",
                    bytes_scanned_limit,
                )

        metrics_backend.increment(
            _SAMPLING_IN_STORAGE_PREFIX + "target_tier",
            1,
            {"referrer": referrer, "tier": str(target_tier)},
        )
        span.set_data(_SAMPLING_IN_STORAGE_PREFIX + "target_tier", target_tier)

        span.set_data(
            _SAMPLING_IN_STORAGE_PREFIX + "estimated_target_tier_bytes_scanned",
            estimated_target_tier_bytes_scanned,
        )
        return target_tier, estimated_target_tier_bytes_scanned


def _is_best_effort_mode(in_msg: T) -> bool:
    return (
        in_msg.meta.HasField("downsampled_storage_config")
        and in_msg.meta.downsampled_storage_config.mode
        == DownsampledStorageConfig.MODE_BEST_EFFORT
    )


def build_snuba_request(
    request: T, query_settings: QuerySettings, build_query: Callable[[T], Query]
) -> SnubaRequest:
    if request.meta.trace_item_type == TraceItemType.TRACE_ITEM_TYPE_LOG:
        team = "ourlogs"
        feature = "ourlogs"
        parent_api = "ourlog_trace_item_table"
    else:
        team = "eap"
        feature = "eap"
        parent_api = "eap_span_samples"

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=build_query(request),
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team=team,
            feature=feature,
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api=parent_api,
        ),
    )


def _run_query_on_most_downsampled_tier(
    request_to_most_downsampled_tier: Request,
    timer: Timer,
    metrics_backend: MetricsBackend,
    referrer: str,
) -> QueryResult:
    with sentry_sdk.start_span(op="_run_query_on_most_downsampled_tier") as span:
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=request_to_most_downsampled_tier,
            timer=timer,
        )
        _record_value_in_span_and_DD(
            span,
            metrics_backend.timing,
            "query_bytes_scanned_from_most_downsampled_tier",
            _get_query_bytes_scanned(res, span),
            {"referrer": referrer},
        )
        _record_value_in_span_and_DD(
            span,
            metrics_backend.timing,
            "query_duration_from_most_downsampled_tier",
            _get_query_duration_ms(res),
            {"referrer": referrer},
        )
        return res


def _emit_estimation_error_info(
    span: Span,
    metrics_backend: MetricsBackend,
    estimated_target_tier_query_bytes_scanned: int,
    res: QueryResult,
    tags: Dict[str, str],
) -> None:
    estimation_error = (
        estimated_target_tier_query_bytes_scanned - _get_query_bytes_scanned(res, span)
    )
    _record_value_in_span_and_DD(
        span,
        metrics_backend.distribution,
        "estimation_error_percentage",
        abs(estimation_error) / _get_query_bytes_scanned(res, span),
        tags,
    )

    estimation_error_metric_name = (
        "over_estimation_error" if estimation_error > 0 else "under_estimation_error"
    )
    _record_value_in_span_and_DD(
        span,
        metrics_backend.distribution,
        estimation_error_metric_name,
        abs(estimation_error),
        tags,
    )


def _skip_storage_routing(in_msg: T) -> bool:
    return (
        not in_msg.meta.HasField("downsampled_storage_config")
        or in_msg.meta.downsampled_storage_config.mode
        == DownsampledStorageConfig.MODE_UNSPECIFIED
    )


def _populate_clickhouse_settings(query_settings: HTTPQuerySettings) -> None:
    query_settings.push_clickhouse_setting(
        "max_execution_time", _get_time_budget() / 1000
    )  # max_execution_time is in seconds
    query_settings.push_clickhouse_setting("timeout_overflow_mode", "break")


def _rerun_query_on_target_tier(
    query_settings: HTTPQuerySettings,
    timer: Timer,
    metrics_backend: MetricsBackend,
    referrer: str,
    target_tier: Tier,
    span: Span,
    in_msg: T,
    build_query: Callable[[T], Query],
    estimated_target_tier_query_bytes_scanned: int,
) -> QueryResult:
    query_settings.set_sampling_tier(target_tier)
    request_to_target_tier = build_snuba_request(in_msg, query_settings, build_query)
    res = run_query(
        dataset=PluggableDataset(name="eap", all_entities=[]),
        request=request_to_target_tier,
        timer=timer,
    )

    if _get_query_bytes_scanned(res, span) != 0:
        _emit_estimation_error_info(
            span,
            metrics_backend,
            estimated_target_tier_query_bytes_scanned,
            res,
            {"referrer": referrer, "tier": str(target_tier)},
        )
    return res


@with_span(op="function")
def run_query_to_correct_tier(
    in_msg: T,
    query_settings: HTTPQuerySettings,
    timer: Timer,
    build_query: Callable[[T], Query],
    metrics_backend: MetricsBackend,
) -> QueryResult:
    if _skip_storage_routing(in_msg):
        return run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=build_snuba_request(in_msg, query_settings, build_query),
            timer=timer,
        )

    referrer = in_msg.meta.referrer

    with sentry_sdk.start_span(op="query_most_downsampled_tier"):
        timer.mark(_START_ESTIMATION_MARK)
        query_settings.set_sampling_tier(_get_most_downsampled_tier())
        request_to_most_downsampled_tier = build_snuba_request(
            in_msg, query_settings, build_query
        )
        res = _run_query_on_most_downsampled_tier(
            request_to_most_downsampled_tier, timer, metrics_backend, referrer
        )

    if _is_best_effort_mode(in_msg):
        with sentry_sdk.start_span(op="redirect_to_target_tier") as span:
            target_tier, estimated_target_tier_query_bytes_scanned = _get_target_tier(
                res, metrics_backend, referrer, timer
            )
            timer.mark(_END_ESTIMATION_MARK)
            _record_value_in_span_and_DD(
                span,
                metrics_backend.timing,
                "estimation_time_overhead",
                timer.get_duration_between_marks(
                    _START_ESTIMATION_MARK, _END_ESTIMATION_MARK
                ),
                {"referrer": referrer, "tier": str(target_tier)},
            )

            _populate_clickhouse_settings(query_settings)

            span.set_data(_SAMPLING_IN_STORAGE_PREFIX + "target_tier", target_tier)
            span.set_data(
                _SAMPLING_IN_STORAGE_PREFIX
                + "estimated_target_tier_query_bytes_scanned",
                estimated_target_tier_query_bytes_scanned,
            )

            if target_tier != _get_most_downsampled_tier():
                res = _rerun_query_on_target_tier(
                    query_settings,
                    timer,
                    metrics_backend,
                    referrer,
                    target_tier,
                    span,
                    in_msg,
                    build_query,
                    estimated_target_tier_query_bytes_scanned,
                )

            _record_value_in_span_and_DD(
                span,
                metrics_backend.distribution,
                f"actual_bytes_scanned_in_target_tier_{target_tier}",
                _get_query_bytes_scanned(res, span),
                tags={"referrer": referrer, "tier": str(target_tier)},
            )
            _record_value_in_span_and_DD(
                span,
                metrics_backend.timing,
                f"time_to_run_query_in_target_tier_{target_tier}",
                _get_query_duration_ms(res),
                tags={"referrer": referrer, "tier": str(target_tier)},
            )

    return res
