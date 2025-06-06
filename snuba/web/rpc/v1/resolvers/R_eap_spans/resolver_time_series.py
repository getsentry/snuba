from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesResponse
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingDecision,
)
from snuba.web.rpc.v1.resolvers import ResolverTimeSeries
from snuba.web.rpc.v1.resolvers.R_eap_items.resolver_time_series import (
    ResolverTimeSeriesEAPItems,
)


class ResolverTimeSeriesEAPSpans(ResolverTimeSeries):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_SPAN

    def resolve(
        self,
        routing_decision: RoutingDecision,
    ) -> TimeSeriesResponse:
        return ResolverTimeSeriesEAPItems().resolve(
            self._timer, self._metrics_backend, routing_decision
        )
