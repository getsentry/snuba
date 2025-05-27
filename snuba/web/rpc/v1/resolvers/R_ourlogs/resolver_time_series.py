from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    TimeSeriesRequest,
    TimeSeriesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.web.rpc.v1.resolvers import ResolverTimeSeries
from snuba.web.rpc.v1.resolvers.R_eap_items.resolver_time_series import (
    ResolverTimeSeriesEAPItems,
)


class ResolverTimeSeriesOurlogs(ResolverTimeSeries):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_LOG

    def resolve(self, in_msg: TimeSeriesRequest) -> TimeSeriesResponse:
        return ResolverTimeSeriesEAPItems().resolve(
            in_msg, self._timer, self._metrics_backend
        )
