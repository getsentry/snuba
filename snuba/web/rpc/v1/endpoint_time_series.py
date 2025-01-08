import math
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, Type

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    DataPoint,
    TimeSeries,
    TimeSeriesRequest,
    TimeSeriesResponse,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import ExtrapolationMode

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    attribute_key_to_expression,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.aggregation import (
    ExtrapolationContext,
    aggregation_to_expression,
    get_average_sample_rate_column,
    get_confidence_interval_column,
    get_count_column,
)

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
        3 * 3600,
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

    def _execute(self, in_msg: TimeSeriesRequest) -> TimeSeriesResponse:
        # TODO: Move this to base
        in_msg.meta.request_id = getattr(in_msg.meta, "request_id", None) or str(
            uuid.uuid4()
        )
        _enforce_no_duplicate_labels(in_msg)
        _validate_time_buckets(in_msg)
        snuba_request = _build_snuba_request(in_msg)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [res],
            [self._timer],
        )

        return TimeSeriesResponse(
            result_timeseries=list(
                _convert_result_timeseries(in_msg, res.result.get("data", []))
            ),
            meta=response_meta,
        )
