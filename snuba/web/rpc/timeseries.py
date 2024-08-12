from datetime import datetime
from typing import Any

from snuba.protobufs.base_messages_pb2 import (
    AggregationType,
    Comparison,
    PentityAggregation,
    PentityFilter,
    PentityFilters,
    RequestInfo,
)
from snuba.protobufs.time_series_pb2 import TimeSeriesRequest
from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query

# TODO: generate
# , TimeSeriesResponse

TimeSeriesResponse = Any


def _agg_type_to_ch_function(agg_type: AggregationType) -> str:
    lookup = {
        AggregationType.AVG: "avg",
        AggregationType.MIN: "min",
        AggregationType.MAX: "max",
        AggregationType.P50: "quantiles(0.5)",
        AggregationType.P90: "quantiles(0.9)",
        AggregationType.P95: "quantiles(0.95)",
        AggregationType.P99: "quantiles(0.99)",
        AggregationType.COUNT: "count",
        AggregationType.UNIQ: "uniq",
    }
    return lookup[agg_type]


def time_series_request(request: TimeSeriesRequest) -> TimeSeriesResponse:
    assert (
        request.pentity_filters.pentity_name == request.pentity_aggregation.pentity_name
    )
    array_str = lambda array: ",".join([str(x) for x in array])
    start_timestamp = datetime.utcfromtimestamp(
        request.request_info.start_timestamp.seconds
    ).strftime("%Y-%m-%d %H:%M:%S")
    end_timestamp = datetime.utcfromtimestamp(
        request.request_info.end_timestamp.seconds
    ).strftime("%Y-%m-%d %H:%M:%S")
    agg_func = _agg_type_to_ch_function(request.pentity_aggregation.aggregation_type)

    snql_query = f"""
MATCH ({request.pentity_filters.pentity_name}) SELECT {agg_func}({request.pentity_aggregation.attribute_name}) BY time WHERE organization_id = {request.request_info.organization_ids[0]} AND project_id IN array({array_str(request.request_info.project_ids)}) AND start_timestamp < toDateTime('{end_timestamp}') AND start_timestamp >= toDateTime('{start_timestamp}') GRANULARITY {request.granularity_secs}
"""
    print("SNQL")
    print(snql_query)
    result = parse_and_run_query(
        body={
            "query": snql_query,
            "tenant_ids": {
                "organization_id": request.request_info.organization_ids[0],
                "project_id": request.request_info.project_ids[0],
                "referrer": request.request_info.referrer,
            },
            "referrer": request.request_info.referrer,
        },
        timer=Timer("timeseries"),
        is_mql=False,
    )
    return result
