from typing import Any, Callable, Mapping, Tuple

from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1alpha.endpoint_aggregate_bucket_pb2 import (
    AggregateBucketRequest,
)
from sentry_protos.snuba.v1alpha.endpoint_span_samples_pb2 import SpanSamplesRequest
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    AttributeValuesRequest,
    TraceItemAttributesRequest,
)

from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.v1alpha.span_samples import span_samples_query
from snuba.web.rpc.v1alpha.timeseries import timeseries_query
from snuba.web.rpc.v1alpha.trace_item_attribute_list import (
    trace_item_attribute_list_query,
)
from snuba.web.rpc.v1alpha.trace_item_attribute_values import (
    trace_item_attribute_values_query,
)

Version = str
EndpointName = str

ALL_RPCS: Mapping[
    Version,
    Mapping[
        EndpointName,
        Tuple[Callable[[Any, Timer], ProtobufMessage], type[ProtobufMessage]],
    ],
] = {
    "v1alpha": {
        "AggregateBucketRequest": (timeseries_query, AggregateBucketRequest),
        "SpanSamplesRequest": (span_samples_query, SpanSamplesRequest),
        "TraceItemAttributesRequest": (
            trace_item_attribute_list_query,
            TraceItemAttributesRequest,
        ),
        "AttributeValuesRequest": (
            trace_item_attribute_values_query,
            AttributeValuesRequest,
        ),
    }
}


def get_rpc_endpoint(
    name: str, version: str
) -> Tuple[Callable[[Any, Timer], ProtobufMessage], type[ProtobufMessage]]:
    return ALL_RPCS[version][name]
