import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    DataPoint,
    TimeSeries,
    TimeSeriesRequest,
    TimeSeriesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

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
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.v1.resolvers import ResolverTimeSeries
from snuba.web.rpc.v1.resolvers.R_uptime_checks.common.aggregation import (
    aggregation_to_expression,
    get_count_column,
)
from snuba.web.rpc.v1.resolvers.R_uptime_checks.common.common import (
    attribute_key_to_expression,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)


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
                group_by_map[col_name] = str(col_value)

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
            if row_data:
                timeseries.data_points.append(
                    DataPoint(
                        data=row_data[timeseries.label],
                        data_present=True,
                    )
                )
            else:
                timeseries.data_points.append(DataPoint(data=0, data_present=False))
    return result_timeseries.values()


def _build_query(request: TimeSeriesRequest) -> Query:
    entity = Entity(
        key=EntityKey("uptime_checks"),
        schema=get_entity(EntityKey("uptime_checks")).get_data_model(),
        sample=None,
    )

    aggregation_columns = [
        SelectedExpression(
            name=aggregation.label, expression=aggregation_to_expression(aggregation)
        )
        for aggregation in request.aggregations
    ]

    additional_context_columns = []
    for aggregation in request.aggregations:
        count_column = get_count_column(aggregation)
        additional_context_columns.append(
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
            # buckets time by granularity according to the start time of the request.
            # time_slot = start_time + (((timestamp - start_time) // granularity) * granularity)
            # Example:
            #   start_time = 1001
            #   end_time = 1901
            #   granularity = 300
            #   timestamps = [1201, 1002, 1302, 1400, 1700]
            #   buckets = [1001, 1301, 1601] # end time not included because it would be filtered out by the request
            SelectedExpression(
                name="time",
                expression=f.toDateTime(
                    f.plus(
                        request.meta.start_timestamp.seconds,
                        f.multiply(
                            f.intDiv(
                                f.minus(
                                    f.toUnixTimestamp(column("scheduled_check_time")),
                                    request.meta.start_timestamp.seconds,
                                ),
                                request.granularity_secs,
                            ),
                            request.granularity_secs,
                        ),
                    ),
                    alias="time_slot",
                ),
            ),
            *aggregation_columns,
            *groupby_columns,
            *additional_context_columns,
        ],
        granularity=request.granularity_secs,
        condition=base_conditions_and(
            request.meta, trace_item_filters_to_expression(request.filter)
        ),
        groupby=[
            column("time_slot"),
            *[attribute_key_to_expression(attr_key) for attr_key in request.group_by],
        ],
        order_by=[
            OrderBy(expression=column("time_slot"), direction=OrderByDirection.ASC)
        ],
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
            parent_api="uptime_check_samples",
        ),
    )


class ResolverTimeSeriesUptimeChecks(ResolverTimeSeries):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_UPTIME_CHECK

    def resolve(self, in_msg: TimeSeriesRequest) -> TimeSeriesResponse:
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
