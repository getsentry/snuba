from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.web.rpc.v1.resolvers import ResolverTraceItemTable
from snuba.web.rpc.v1.resolvers.R_eap_items.resolver_trace_item_table import (
    ResolverTraceItemTableEAPItems,
)


class ResolverTraceItemTableEAPSpans(ResolverTraceItemTable):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_SPAN

    def resolve(self, in_msg: TraceItemTableRequest) -> TraceItemTableResponse:
        return ResolverTraceItemTableEAPItems().resolve(
            in_msg, self._timer, self._metrics_backend
        )
