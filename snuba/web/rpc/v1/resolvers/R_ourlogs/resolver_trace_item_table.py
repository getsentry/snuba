from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.state import get_int_config
from snuba.web.rpc.v1.resolvers import ResolverTraceItemTable
from snuba.web.rpc.v1.resolvers.R_eap_items.resolver_trace_item_table import (
    ResolverTraceItemTableEAPItems,
)
from snuba.web.rpc.v1.resolvers.R_ourlogs.old_resolvers.old_resolver_trace_item_table import (
    OldResolverTraceItemTableOurlogs,
)


class ResolverTraceItemTableOurlogs(ResolverTraceItemTable):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_LOG

    def resolve(self, in_msg: TraceItemTableRequest) -> TraceItemTableResponse:
        use_new_logs_resolver = bool(get_int_config("use_new_logs_resolver", default=0))
        if use_new_logs_resolver:
            res = ResolverTraceItemTableEAPItems().resolve(in_msg)
            # option 2 at this point convert the timestamp alias
            return res
        else:
            return OldResolverTraceItemTableOurlogs().resolve(in_msg)
