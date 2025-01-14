from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    TimeSeriesRequest,
    TimeSeriesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemName

from snuba.web.rpc.v1.resolvers import ResolverTimeSeries


class ResolverTimeSeriesEAPSpans(ResolverTimeSeries):
    @classmethod
    def trace_item_name(cls) -> TraceItemName.ValueType:
        return TraceItemName.TRACE_ITEM_NAME_EAP_SPANS

    def resolve(self, in_msg: TimeSeriesRequest) -> TimeSeriesResponse:
        raise NotImplementedError("aggregation is not supported for logs")
