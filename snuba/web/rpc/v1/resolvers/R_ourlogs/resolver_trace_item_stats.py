from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import (
    TraceItemStatsRequest,
    TraceItemStatsResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers import ResolverTraceItemStats


class ResolverTraceItemStatsOurlogs(ResolverTraceItemStats):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_LOG

    def resolve(self, in_msg: TraceItemStatsRequest) -> TraceItemStatsResponse:
        raise BadSnubaRPCRequestException("aggregation is not supported for logs")
