import uuid
from typing import Callable, Dict, Tuple, TypeVar, cast

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest

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
from snuba.web import QueryResult
from snuba.web.query import run_query

T = TypeVar("T", TimeSeriesRequest, TraceItemTableRequest)


DOWNSAMPLING_TIER_MULTIPLIERS: Dict[Tier, int] = {
    Tier.TIER_512: 1,
    Tier.TIER_64: 8,
    Tier.TIER_8: 64,
    Tier.TIER_1: 512,
}


def _get_time_budget() -> float:
    sentry_timeout_ms = cast(
        int, state.get_int_config("sampling_in_storage_sentry_timeout", default=30000)
    )  # 30s = 30000ms
    error_budget_ms = cast(
        int, state.get_int_config("sampling_in_storage_error_budget", default=5000)
    )  # 5s = 5000ms
    return sentry_timeout_ms - error_budget_ms


def _get_query_duration(timer: Timer) -> float:
    return timer.get_duration_between_marks("right_before_execute", "execute")


def _get_most_downsampled_tier() -> Tier:
    return sorted(Tier, reverse=True)[0]


def _get_target_tier(
    timer: Timer, metrics_backend: MetricsBackend, referrer: str
) -> Tuple[Tier, float]:
    most_downsampled_query_duration_ms = _get_query_duration(timer)
    print("durationnn", most_downsampled_query_duration_ms)

    target_tier = _get_most_downsampled_tier()
    estimated_target_tier_query_duration = most_downsampled_query_duration_ms * cast(
        int, DOWNSAMPLING_TIER_MULTIPLIERS.get(target_tier)
    )
    for tier in sorted(Tier, reverse=True)[:-1]:
        estimated_query_duration_to_this_tier = (
            most_downsampled_query_duration_ms
            * cast(int, DOWNSAMPLING_TIER_MULTIPLIERS.get(tier))
        )
        metrics_backend.timing(
            "sampling_in_storage_estimated_query_duration",
            estimated_query_duration_to_this_tier,
            tags={"referrer": referrer, "tier": str(tier)},
        )
        if (
            estimated_query_duration_to_this_tier
            <= _get_time_budget() - most_downsampled_query_duration_ms
        ):
            target_tier = tier
            estimated_target_tier_query_duration = estimated_query_duration_to_this_tier

    metrics_backend.timing(
        "sampling_in_storage_routed_tier",
        target_tier,
        tags={"referrer": referrer},
    )
    return target_tier, estimated_target_tier_query_duration


def _is_best_effort_mode(in_msg: T) -> bool:
    return (
        in_msg.meta.HasField("downsampled_storage_config")
        and in_msg.meta.downsampled_storage_config.mode
        == DownsampledStorageConfig.MODE_BEST_EFFORT
    )


def _record_actual_query_duration(
    metrics_backend: MetricsBackend, timer: Timer, tags: Dict[str, str]
) -> None:
    metrics_backend.timing(
        "sampling_in_storage_actual_query_duration",
        _get_query_duration(timer),
        tags=tags,
    )


def build_snuba_request(
    request: T, query_settings: QuerySettings, build_query: Callable[[T], Query]
) -> SnubaRequest:
    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=build_query(request),
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="eap_span_samples",
        ),
    )


def _run_query_on_most_downsampled_tier(
    request_to_most_downsampled_tier: Request,
    timer: Timer,
    metrics_backend: MetricsBackend,
    referrer: str,
) -> QueryResult:
    res = run_query(
        dataset=PluggableDataset(name="eap", all_entities=[]),
        request=request_to_most_downsampled_tier,
        timer=timer,
    )
    metrics_backend.timing(
        "sampling_in_storage_query_duration_from_most_downsampled_tier",
        _get_query_duration(timer),
        tags={"referrer": referrer},
    )
    return res


def run_query_to_correct_tier(
    in_msg: T,
    query_settings: HTTPQuerySettings,
    timer: Timer,
    build_query: Callable[[T], Query],
    metrics_backend: MetricsBackend,
) -> QueryResult:
    if (
        not in_msg.meta.HasField("downsampled_storage_config")
        or in_msg.meta.downsampled_storage_config.mode
        == DownsampledStorageConfig.MODE_UNSPECIFIED
    ):
        return run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=build_snuba_request(in_msg, query_settings, build_query),
            timer=timer,
        )

    query_settings.set_sampling_tier(_get_most_downsampled_tier())

    referrer = in_msg.meta.referrer

    request_to_most_downsampled_tier = build_snuba_request(
        in_msg, query_settings, build_query
    )
    res = _run_query_on_most_downsampled_tier(
        request_to_most_downsampled_tier, timer, metrics_backend, referrer
    )

    if _is_best_effort_mode(in_msg):
        query_settings.push_clickhouse_setting(
            "max_execution_time",
            _get_time_budget() / 1000,
        )
        query_settings.push_clickhouse_setting("timeout_overflow_mode", "break")
        target_tier, estimated_target_tier_query_duration = _get_target_tier(
            timer, metrics_backend, referrer
        )

        if target_tier == _get_most_downsampled_tier():
            _record_actual_query_duration(
                metrics_backend,
                timer,
                tags={"referrer": referrer, "tier": str(target_tier)},
            )
            return res

        query_settings.set_sampling_tier(target_tier)

        request_to_target_tier = build_snuba_request(
            in_msg, query_settings, build_query
        )

        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=request_to_target_tier,
            timer=timer,
        )
        _record_actual_query_duration(
            metrics_backend,
            timer,
            tags={"referrer": referrer, "tier": str(target_tier)},
        )
        metrics_backend.timing(
            "sampling_in_storage_estimation_error",
            estimated_target_tier_query_duration - _get_query_duration(timer),
            tags={"referrer": referrer, "tier": str(target_tier)},
        )

    return res
