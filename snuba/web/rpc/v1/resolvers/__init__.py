from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    TimeSeriesRequest,
    TimeSeriesResponse,
)
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
    TraceItemAttributeNamesResponse,
    TraceItemAttributeValuesRequest,
    TraceItemAttributeValuesResponse,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    TraceItemTableRequest,
    TraceItemTableResponse,
)

from snuba.web.rpc import TraceItemDataResolver


class ResolverTraceItemTable(
    TraceItemDataResolver[TraceItemTableRequest, TraceItemTableResponse]
):
    pass


class ResolverTimeSeries(TraceItemDataResolver[TimeSeriesRequest, TimeSeriesResponse]):
    pass


class ResolverAttributeNames(
    TraceItemDataResolver[
        TraceItemAttributeNamesRequest, TraceItemAttributeNamesResponse
    ]
):
    pass


class ResolverAttributeValues(
    TraceItemDataResolver[
        TraceItemAttributeValuesRequest, TraceItemAttributeValuesResponse
    ]
):
    pass


# TODO: Traces, subscriptions
