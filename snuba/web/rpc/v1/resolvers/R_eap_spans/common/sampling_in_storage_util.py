import uuid
from typing import Callable, Dict, Tuple, TypeVar, cast

import sentry_sdk
from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
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


def _get_bytes_scanned_limit() -> int:
    return cast(
        int,
        state.get_int_config(
            "sampling_in_storage_bytes_scanned_per_query_limit", default=161061273600
        ),
    )  # 150 gigabytes is default


def _get_query_bytes_scanned(res: QueryResult) -> int:
    return cast(int, res.result.get("profile", {}).get("progress_bytes", 0))  # type: ignore


def _get_most_downsampled_tier() -> Tier:
    return sorted(Tier, reverse=True)[0]


def _get_target_tier(
    most_downsampled_res: QueryResult, metrics_backend: MetricsBackend, referrer: str
) -> Tuple[Tier, float]:
    with sentry_sdk.start_span(op="_get_target_tier") as span:
        most_downsampled_query_bytes_scanned = _get_query_bytes_scanned(
            most_downsampled_res
        )
        span.set_data(
            "most_downsampled_query_bytes_scanned", most_downsampled_query_bytes_scanned
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

                tier_specific_span.set_data(
                    "estimated_query_bytes_scanned_to_this_tier",
                    estimated_query_bytes_scanned_to_this_tier,
                )
                metrics_backend.timing(
                    "sampling_in_storage_estimated_query_bytes_scanned",
                    estimated_query_bytes_scanned_to_this_tier,
                    tags={"referrer": referrer, "tier": str(tier)},
                )

                bytes_scanned_limit = _get_bytes_scanned_limit()
                if estimated_query_bytes_scanned_to_this_tier <= bytes_scanned_limit:
                    target_tier = tier
                    estimated_target_tier_bytes_scanned = (
                        estimated_query_bytes_scanned_to_this_tier
                    )

                tier_specific_span.set_data("target_tier", target_tier)
                tier_specific_span.set_data("bytes_scanned_limit", bytes_scanned_limit)

        metrics_backend.timing(
            "sampling_in_storage_routed_tier",
            target_tier,
            tags={"referrer": referrer},
        )
        span.set_data("target_tier", target_tier)
        span.set_data(
            "estimated_target_tier_bytes_scanned", estimated_target_tier_bytes_scanned
        )
        return target_tier, estimated_target_tier_bytes_scanned


def _is_best_effort_mode(in_msg: T) -> bool:
    return (
        in_msg.meta.HasField("downsampled_storage_config")
        and in_msg.meta.downsampled_storage_config.mode
        == DownsampledStorageConfig.MODE_BEST_EFFORT
    )


def _record_actual_bytes_scanned(
    span: Span, metrics_backend: MetricsBackend, res: QueryResult, tags: Dict[str, str]
) -> None:
    actual_query_bytes_scanned = _get_query_bytes_scanned(res)
    span.set_data("tier", tags.get("tier", -1))
    span.set_data("actual_query_bytes_scanned", actual_query_bytes_scanned)
    metrics_backend.timing(
        "sampling_in_storage_actual_query_bytes_scanned",
        actual_query_bytes_scanned,
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
    with sentry_sdk.start_span(op="_run_query_on_most_downsampled_tier") as span:
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=request_to_most_downsampled_tier,
            timer=timer,
        )
        query_bytes_scanned_from_most_downsampled_tier = _get_query_bytes_scanned(res)
        span.set_data(
            "query_bytes_scanned_from_most_downsampled_tier",
            query_bytes_scanned_from_most_downsampled_tier,
        )
        metrics_backend.timing(
            "sampling_in_storage_query_bytes_scanned_from_most_downsampled_tier",
            query_bytes_scanned_from_most_downsampled_tier,
            tags={"referrer": referrer},
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

    with sentry_sdk.start_span(op="query_most_downsampled_tier"):

        query_settings.set_sampling_tier(_get_most_downsampled_tier())

        referrer = in_msg.meta.referrer

        request_to_most_downsampled_tier = build_snuba_request(
            in_msg, query_settings, build_query
        )
        res = _run_query_on_most_downsampled_tier(
            request_to_most_downsampled_tier, timer, metrics_backend, referrer
        )

    if _is_best_effort_mode(in_msg):
        with sentry_sdk.start_span(op="redirect_to_target_tier") as span:
            query_settings.push_clickhouse_setting(
                "max_execution_time",
                _get_time_budget() / 1000,
            )
            query_settings.push_clickhouse_setting("timeout_overflow_mode", "break")
            target_tier, estimated_target_tier_query_bytes_scanned = _get_target_tier(
                res, metrics_backend, referrer
            )

            span.set_data("target_tier", target_tier)
            span.set_data(
                "estimated_target_tier_query_bytes_scanned",
                estimated_target_tier_query_bytes_scanned,
            )

            if target_tier == _get_most_downsampled_tier():
                _record_actual_bytes_scanned(
                    span,
                    metrics_backend,
                    res,
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
            _record_actual_bytes_scanned(
                span,
                metrics_backend,
                res,
                tags={"referrer": referrer, "tier": str(target_tier)},
            )
            estimation_error = (
                estimated_target_tier_query_bytes_scanned
                - _get_query_bytes_scanned(res)
            )
            metrics_backend.timing(
                "sampling_in_storage_estimation_error",
                estimation_error,
                tags={"referrer": referrer, "tier": str(target_tier)},
            )

            span.set_data("estimation_error", estimation_error)

    return res
