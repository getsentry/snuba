from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import (
    TraceItemStatsRequest,
    TraceItemStatsResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.web.rpc.v1.resolvers import ResolverTraceItemStats
from snuba.web.rpc.v1.resolvers.R_eap_items.resolver_trace_item_stats import (
    ResolverTraceItemStatsEAPItems,
)


class ResolverTraceItemStatsEAPSpans(ResolverTraceItemStats):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_SPAN

    def resolve(self, in_msg: TraceItemStatsRequest) -> TraceItemStatsResponse:
        return ResolverTraceItemStatsEAPItems().resolve(in_msg, self._timer)
