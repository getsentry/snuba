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
from snuba.query.dsl import column
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.aggregation import (
    ExtrapolationMeta,
    aggregation_to_expression,
    get_average_sample_rate_column,
    get_count_column,
    get_upper_confidence_column,
)
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

_VALID_GRANULARITY_SECS = set(
    [
        15,
        30,
        60,  # seconds
        2 * 60,
        5 * 60,
        10 * 60,
        30 * 60,  # minutes
        1 * 3600,
        3 * 3600,
        12 * 3600,
        24 * 3600,  # hours
    ]
)

_MAX_BUCKETS_IN_REQUEST = 1000


def _convert_result_timeseries(
    request: TimeSeriesRequest, data: list[Dict[str, Any]]
) -> Iterable[TimeSeries]:
    """This function takes the results of the clickhouse query and converts it to a list of TimeSeries objects. It also handles
    zerofilling data points where data was not present for a specific bucket.

    Example:
    data is a list of dictionaries that look like this:

    >>> [
    >>>     {"time": "2024-4-20 16:20:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g1", "group_by_attr_2": "a1"}
    >>>     {"time": "2024-4-20 16:20:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g1", "group_by_attr_2": "a2"}
    >>>     {"time": "2024-4-20 16:20:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g2", "group_by_attr_2": "a1"}
    >>>     {"time": "2024-4-20 16:20:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g2", "group_by_attr_2": "a2"}
    >>>     # next time bucket starts below

    >>>     {"time": "2024-4-20 16:21:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g1", "group_by_attr_2": "a1"}
    >>>     # here you can see that not every timeseries had data in every time bucket
    >>>     {"time": "2024-4-20 16:22:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g1", "group_by_attr_2": "a2"}
    >>>     {"time": "2024-4-20 16:23:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g2", "group_by_attr_2": "a1"}
    >>>     {"time": "2024-4-20 16:24:00", "sum(sentry.duration)": 1235, "p95(sentry.duration)": 123456, "group_by_attr_1": "g2", "group_by_attr_2": "a2"}

    >>>     ...
    >>> ]

    In this example we have 8 different timeseries and they are all sparse:

        sum(sentry.duration), group_by_attributes = {"group_by_attr_1": "g1", "group_by_attr_2": "a1"}
        sum(sentry.duration), group_by_attributes = {"group_by_attr_1": "g1", "group_by_attr_2": "a2"}
        sum(sentry.duration), group_by_attributes = {"group_by_attr_1": "g2", "group_by_attr_2": "a1"}
        sum(sentry.duration), group_by_attributes = {"group_by_attr_1": "g2", "group_by_attr_2": "a2"}


        p95(sentry.duration), group_by_attributes = {"group_by_attr_1": "g1", "group_by_attr_2": "a1"}
        p95(sentry.duration), group_by_attributes = {"group_by_attr_1": "g1", "group_by_attr_2": "a2"}
        p95(sentry.duration), group_by_attributes = {"group_by_attr_1": "g2", "group_by_attr_2": "a1"}
        p95(sentry.duration), group_by_attributes = {"group_by_attr_1": "g2", "group_by_attr_2": "a2"}

    Returns:
        an Iterable of TimeSeries objects where each possible bucket has a DataPoint with `data_present` set correctly

    """

    # to convert the results, need to know which were the groupby columns and which ones
    # were aggregations
    aggregation_labels = set([agg.label for agg in request.aggregations])
    group_by_labels = set([attr.name for attr in request.group_by])

    # create a mapping with (all the group by attribute key,val pairs as strs, label name)
    # In the example in the docstring it would look like:
    # { ("group_by_attr_1,g1|group_by_attr_2,g2", "sum(sentry.duration"): TimeSeries()}
    result_timeseries: dict[tuple[str, str], TimeSeries] = {}

    # create a mapping for each timeseries of timestamp: row to fill data points not returned in the query
    # {
    #   ("group_by_attr_1,g1|group_by_attr_2,g2", "sum(sentry.duration"): {
    #       time_converted_to_integer_timestamp: row_data_for_that_time_bucket
    #   }
    # }
    result_timeseries_timestamp_to_row: defaultdict[
        tuple[str, str], dict[int, Dict[str, Any]]
    ] = defaultdict(dict)

    query_duration = (
        request.meta.end_timestamp.seconds - request.meta.start_timestamp.seconds
    )
    time_buckets = [
        Timestamp(seconds=(request.meta.start_timestamp.seconds) + secs)
        for secs in range(0, query_duration, request.granularity_secs)
    ]

    # this loop fill in our pre-computed dictionaries so that we can zerofill later
    for row in data:
        group_by_map = {}

        for col_name, col_value in row.items():
            if col_name in group_by_labels:
                group_by_map[col_name] = col_value

        group_by_key = "|".join([f"{k},{v}" for k, v in group_by_map.items()])
        for col_name in aggregation_labels:
            if not result_timeseries.get((group_by_key, col_name), None):
                result_timeseries[(group_by_key, col_name)] = TimeSeries(
                    group_by_attributes=group_by_map,
                    label=col_name,
                    buckets=time_buckets,
                )
            result_timeseries_timestamp_to_row[(group_by_key, col_name)][
                int(datetime.fromisoformat(row["time"]).timestamp())
            ] = row

    # Go through every possible time bucket in the query, if there's row data for it, fill in its data
    # otherwise put a dummy datapoint in

    for bucket in time_buckets:
        for timeseries_key, timeseries in result_timeseries.items():
            row_data = result_timeseries_timestamp_to_row.get(timeseries_key, {}).get(
                bucket.seconds
            )
            if not row_data:
                timeseries.data_points.append(DataPoint(data=0, data_present=False))
            else:
                extrapolation_meta = ExtrapolationMeta.from_row(
                    row_data, timeseries.label
                )
                timeseries.data_points.append(
                    DataPoint(
                        data=row_data[timeseries.label],
                        data_present=True,
                        avg_sampling_rate=extrapolation_meta.avg_sampling_rate,
                        reliability=extrapolation_meta.reliability,
                    )
                )
    return result_timeseries.values()


def _build_query(request: TimeSeriesRequest) -> Query:
    # TODO: This is hardcoded still
    entity = Entity(
        key=EntityKey("eap_spans"),
        schema=get_entity(EntityKey("eap_spans")).get_data_model(),
        sample=None,
    )

    aggregation_columns = [
        SelectedExpression(
            name=aggregation.label, expression=aggregation_to_expression(aggregation)
        )
        for aggregation in request.aggregations
    ]

    extrapolation_columns = []
    for aggregation in request.aggregations:
        if (
            aggregation.extrapolation_mode
            == ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED
        ):
            upper_confidence_column = get_upper_confidence_column(aggregation)
            if upper_confidence_column is not None:
                extrapolation_columns.append(
                    SelectedExpression(
                        name=upper_confidence_column.alias,
                        expression=upper_confidence_column,
                    )
                )

            average_sample_rate_column = get_average_sample_rate_column(aggregation)
            count_column = get_count_column(aggregation)
            extrapolation_columns.append(
                SelectedExpression(
                    name=average_sample_rate_column.alias,
                    expression=average_sample_rate_column,
                )
            )
            extrapolation_columns.append(
                SelectedExpression(name=count_column.alias, expression=count_column)
            )

    groupby_columns = [
        SelectedExpression(
            name=attr_key.name, expression=attribute_key_to_expression(attr_key)
        )
        for attr_key in request.group_by
    ]

    res = Query(
        from_clause=entity,
        selected_columns=[
            SelectedExpression(name="time", expression=column("time", alias="time")),
            *aggregation_columns,
            *groupby_columns,
            *extrapolation_columns,
        ],
        granularity=request.granularity_secs,
        condition=base_conditions_and(
            request.meta, trace_item_filters_to_expression(request.filter)
        ),
        groupby=[
            column("time"),
            *[attribute_key_to_expression(attr_key) for attr_key in request.group_by],
        ],
        order_by=[OrderBy(expression=column("time"), direction=OrderByDirection.ASC)],
    )
    treeify_or_and_conditions(res)
    return res


def _build_snuba_request(request: TimeSeriesRequest) -> SnubaRequest:
    query_settings = (
        setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
    )

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=_build_query(request),
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="eap_span_samples",
        ),
    )


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
