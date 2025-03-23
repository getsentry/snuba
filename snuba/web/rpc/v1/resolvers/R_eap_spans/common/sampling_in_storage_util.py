from typing import cast

from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest

from snuba.downsampled_storage_tiers import Tier
from snuba.query.query_settings import HTTPQuerySettings
from snuba.web.rpc.common.debug_info import setup_trace_query_settings

SENTRY_TIMEOUT = 30000  # 30s = 30000ms
ERROR_BUDGET = 5000  # 5s

TIER_MULTIPLIERS = {
    Tier.TIER_512: 1,
    Tier.TIER_64: 8,
    Tier.TIER_8: 64,
    Tier.TIER_1: 512,
}


def construct_query_settings(
    in_msg: TimeSeriesRequest | TraceItemTableRequest,
) -> HTTPQuerySettings:
    query_settings = (
        setup_trace_query_settings() if in_msg.meta.debug else HTTPQuerySettings()
    )
    if in_msg.meta.HasField("downsampled_storage_config"):
        if (
            in_msg.meta.downsampled_storage_config.mode
            == DownsampledStorageConfig.MODE_PREFLIGHT
        ):
            query_settings.set_sampling_tier(Tier.TIER_512)

    return query_settings


def get_target_tier(most_downsampled_query_duration_ms: float) -> Tier:
    target_tier = Tier.TIER_NO_TIER
    for tier in sorted(Tier, reverse=True)[:-1]:
        estimated_query_duration_to_this_tier = (
            most_downsampled_query_duration_ms * cast(int, TIER_MULTIPLIERS.get(tier))
        )
        if estimated_query_duration_to_this_tier <= SENTRY_TIMEOUT - ERROR_BUDGET:
            target_tier = tier
        print(tier, estimated_query_duration_to_this_tier)
    return target_tier
