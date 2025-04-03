from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.state import get_int_config
from snuba.web.rpc.v1.resolvers import ResolverTraceItemTable
from snuba.web.rpc.v1.resolvers.R_eap_spans.resolver_trace_item_table import (
    ResolverTraceItemTableEAPSpans,
)


class ResolverTraceItemTableOurlogs(ResolverTraceItemTable):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_LOG

    def resolve(self, in_msg: TraceItemTableRequest) -> TraceItemTableResponse:
        use_new_logs_resolver = bool(get_int_config("use_new_logs_resolver", default=0))
        if use_new_logs_resolver:
            res = ResolverTraceItemTableEAPSpans().resolve(in_msg)
            # option 2 at this point convert the timestamp alias
            return res
        else:
            raise NotImplementedError("todo add back the old one and use it")
