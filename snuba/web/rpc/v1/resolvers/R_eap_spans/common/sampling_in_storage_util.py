import uuid
from typing import Callable, TypeVar, cast

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest

from snuba.attribution import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.downsampled_storage_tiers import Tier
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.query import run_query

T = TypeVar("T", TimeSeriesRequest, TraceItemTableRequest)


SENTRY_TIMEOUT = 30000  # 30s = 30000ms
ERROR_BUDGET = 5000  # 5s

DOWNSAMPLING_TIER_MULTIPLIERS = {
    Tier.TIER_512: 1,
    Tier.TIER_64: 8,
    Tier.TIER_8: 64,
    Tier.TIER_1: 512,
}


def _get_query_duration(timer) -> int:
    return timer.get_duration_between_marks("right_before_execute", "execute")


def _get_target_tier(timer: Timer) -> Tier:
    most_downsampled_query_duration_ms = _get_query_duration(timer)

    target_tier = Tier.TIER_NO_TIER
    for tier in sorted(Tier, reverse=True)[:-1]:
        estimated_query_duration_to_this_tier = (
            most_downsampled_query_duration_ms
            * cast(int, DOWNSAMPLING_TIER_MULTIPLIERS.get(tier))
        )
        if estimated_query_duration_to_this_tier <= SENTRY_TIMEOUT - ERROR_BUDGET:
            target_tier = tier
    return target_tier


def _is_best_effort_mode(in_msg: T) -> bool:
    return (
        in_msg.meta.HasField("downsampled_storage_config")
        and in_msg.meta.downsampled_storage_config.mode
        == DownsampledStorageConfig.MODE_BEST_EFFORT
    )


def _enough_time_budget_to_at_least_run_next_tier(timer: Timer) -> bool:
    most_downsampled_query_duration_ms = _get_query_duration(timer)
    return most_downsampled_query_duration_ms * 9 < SENTRY_TIMEOUT - ERROR_BUDGET


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


def run_query_to_correct_tier(
    in_msg: T,
    query_settings: HTTPQuerySettings,
    timer: Timer,
    build_query: Callable[[T], Query],
) -> QueryResult:
    if not in_msg.meta.HasField("downsampled_storage_config"):
        return run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=build_snuba_request(in_msg, query_settings, build_query),
            timer=timer,
        )

    query_settings.set_sampling_tier(Tier.TIER_512)

    request_to_most_downsampled_tier = build_snuba_request(
        in_msg, query_settings, build_query
    )

    res = run_query(
        dataset=PluggableDataset(name="eap", all_entities=[]),
        request=request_to_most_downsampled_tier,
        timer=timer,
    )

    if _is_best_effort_mode(in_msg) and _enough_time_budget_to_at_least_run_next_tier(
        timer
    ):
        query_settings.push_clickhouse_setting(
            "max_execution_time", (SENTRY_TIMEOUT - ERROR_BUDGET) / 1000
        )
        query_settings.push_clickhouse_setting("timeout_overflow_mode", "break")
        query_settings.set_sampling_tier(_get_target_tier(timer))

        request_to_target_tier = build_snuba_request(
            in_msg, query_settings, build_query
        )

        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=request_to_target_tier,
            timer=timer,
        )

    return res
