from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest

from snuba.downsampled_storage_tiers import Tier
from snuba.query.query_settings import HTTPQuerySettings
from snuba.web import QueryResult
from snuba.web.rpc.common.debug_info import setup_trace_query_settings


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


def add_sampling_tier_to_query_stats(
    query_result: QueryResult, query_settings: HTTPQuerySettings
) -> None:
    stats = dict(query_result.extra["stats"])
    stats["sampling_tier"] = query_settings.get_sampling_tier()
    query_result.extra["stats"] = stats
