import math
import uuid
from typing import Type

from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    TimeSeriesRequest,
    TimeSeriesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.web.rpc import RPCEndpoint, TraceItemDataResolver
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers import ResolverTimeSeries

_VALID_GRANULARITY_SECS = set(
    [
        15,
        30,
        60,  # seconds
        2 * 60,
        5 * 60,
        10 * 60,
        15 * 60,
        30 * 60,  # minutes
        1 * 3600,
        2 * 3600,
        3 * 3600,
        4 * 3600,
        12 * 3600,
        24 * 3600,  # hours
    ]
)

# MAX 5 minute granularity over 7 days
_MAX_BUCKETS_IN_REQUEST = 2016


def _enforce_no_duplicate_labels(request: TimeSeriesRequest) -> None:
    labels = set()

    for agg in request.aggregations:
        if agg.label in labels:
            raise BadSnubaRPCRequestException(f"duplicate label {agg.label} in request")
        labels.add(agg.label)


def _validate_time_buckets(request: TimeSeriesRequest) -> None:
    if request.meta.start_timestamp.seconds > request.meta.end_timestamp.seconds:
        raise BadSnubaRPCRequestException("start timestamp is after end timestamp")
    if request.granularity_secs == 0:
        raise BadSnubaRPCRequestException("granularity of 0 is invalid")

    if request.granularity_secs not in _VALID_GRANULARITY_SECS:
        raise BadSnubaRPCRequestException(
            f"Granularity of {request.granularity_secs} is not valid, valid granularity_secs: {sorted(_VALID_GRANULARITY_SECS)}"
        )
    request_duration = (
        request.meta.end_timestamp.seconds - request.meta.start_timestamp.seconds
    )
    num_buckets = request_duration / request.granularity_secs
    if num_buckets > _MAX_BUCKETS_IN_REQUEST:
        raise BadSnubaRPCRequestException(
            f"This request is asking for too many datapoints ({num_buckets}, please raise your granularity_secs or shorten your time window"
        )
    if num_buckets < 1:
        raise BadSnubaRPCRequestException(
            "This request will return no datapoints lower your granularity or lengthen your time window"
        )

    ceil_num_buckets = math.ceil(num_buckets)
    # if the granularity and time windoes don't match up evenly, adjust the window to include another data point
    if num_buckets != ceil_num_buckets:
        request.meta.end_timestamp.seconds = request.meta.start_timestamp.seconds + (
            ceil_num_buckets * request.granularity_secs
        )


class EndpointTimeSeries(RPCEndpoint[TimeSeriesRequest, TimeSeriesResponse]):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TimeSeriesRequest]:
        return TimeSeriesRequest

    @classmethod
    def response_class(cls) -> Type[TimeSeriesResponse]:
        return TimeSeriesResponse

    def get_resolver(
        self, trace_item_type: TraceItemType.ValueType
    ) -> TraceItemDataResolver[TimeSeriesRequest, TimeSeriesResponse]:
        return ResolverTimeSeries.get_from_trace_item_type(trace_item_type)(
            timer=self._timer, metrics_backend=self._metrics_backend
        )

    def _execute(self, in_msg: TimeSeriesRequest) -> TimeSeriesResponse:
        # TODO: Move this to base
        in_msg.meta.request_id = getattr(in_msg.meta, "request_id", None) or str(
            uuid.uuid4()
        )
        _enforce_no_duplicate_labels(in_msg)
        _validate_time_buckets(in_msg)

        if in_msg.meta.trace_item_type == TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED:
            raise BadSnubaRPCRequestException(
                "This endpoint requires meta.trace_item_type to be set (are you requesting spans? logs?)"
            )
        resolver = self.get_resolver(in_msg.meta.trace_item_type)
        return resolver.resolve(in_msg)
