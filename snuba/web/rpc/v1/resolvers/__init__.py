import os

from sentry_protos.snuba.v1.endpoint_get_trace_pb2 import (
    GetTraceRequest,
    GetTraceResponse,
)
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

from snuba.utils.registered_class import import_submodules_in_directory
from snuba.web.rpc import TraceItemDataResolver


class ResolverTraceItemTable(
    TraceItemDataResolver[TraceItemTableRequest, TraceItemTableResponse]
):
    @classmethod
    def endpoint_name(cls) -> str:
        return "TraceItemTable"


class ResolverTimeSeries(TraceItemDataResolver[TimeSeriesRequest, TimeSeriesResponse]):
    @classmethod
    def endpoint_name(cls) -> str:
        return "TimeSeries"


class ResolverAttributeNames(
    TraceItemDataResolver[
        TraceItemAttributeNamesRequest, TraceItemAttributeNamesResponse
    ]
):
    @classmethod
    def endpoint_name(cls) -> str:
        return "AttributeNames"


class ResolverAttributeValues(
    TraceItemDataResolver[
        TraceItemAttributeValuesRequest, TraceItemAttributeValuesResponse
    ]
):
    @classmethod
    def endpoint_name(cls) -> str:
        return "AttributeValues"


class ResolverGetTrace(
    TraceItemDataResolver[
        GetTraceRequest,
        GetTraceResponse,
    ]
):
    @classmethod
    def endpoint_name(cls) -> str:
        return "GetTrace"


# TODO: Traces, subscriptions


_TO_IMPORT = {}

for f in os.listdir(os.path.dirname(os.path.realpath(__file__))):
    if f.startswith("R_"):
        _TO_IMPORT[f] = os.path.join(os.path.dirname(os.path.realpath(__file__)), f)


for v, module_path in _TO_IMPORT.items():
    import_submodules_in_directory(module_path, f"snuba.web.rpc.v1.resolvers.{v}")
