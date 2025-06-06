from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableResponse
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingDecision,
)
from snuba.web.rpc.v1.resolvers import ResolverTraceItemTable
from snuba.web.rpc.v1.resolvers.R_eap_items.resolver_trace_item_table import (
    ResolverTraceItemTableEAPItems,
)


class ResolverTraceItemTableOurlogs(ResolverTraceItemTable):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_LOG

    def resolve(self, routing_decision: RoutingDecision) -> TraceItemTableResponse:
        return ResolverTraceItemTableEAPItems().resolve(
            self._timer, self._metrics_backend, routing_decision
        )
