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
from snuba.web.rpc.span_samples import span_samples_query
from snuba.web.rpc.timeseries import timeseries_query
from snuba.web.rpc.trace_item_attributes import trace_items_attributes_query
from snuba.web.rpc.trace_item_values import trace_item_values_query

ALL_RPCS: Mapping[
    str, Tuple[Callable[[Any, Timer], ProtobufMessage], type[ProtobufMessage]]
] = {
    "AggregateBucketRequest": (timeseries_query, AggregateBucketRequest),
    "SpanSamplesRequest": (span_samples_query, SpanSamplesRequest),
    "TraceItemAttributesRequest": (
        trace_items_attributes_query,
        TraceItemAttributesRequest,
    ),
    "AttributeValuesRequest": (trace_item_values_query, AttributeValuesRequest),
}
