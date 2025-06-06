from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import TraceItemStatsResponse
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingDecision,
)
from snuba.web.rpc.v1.resolvers import ResolverTraceItemStats
from snuba.web.rpc.v1.resolvers.R_eap_items.resolver_trace_item_stats import (
    ResolverTraceItemStatsEAPItems,
)


class ResolverTraceItemStatsOurlogs(ResolverTraceItemStats):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_LOG

    def resolve(self, routing_decision: RoutingDecision) -> TraceItemStatsResponse:
        return ResolverTraceItemStatsEAPItems().resolve(
            routing_decision,
            self._timer,
        )
