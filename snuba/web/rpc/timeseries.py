from snuba.protobufs.time_series_pb2 import TimeSeriesRequest
from typing import Any
from snuba.web.query import parse_and_run_query
from snuba.utils.metrics.timer import Timer
from datetime import datetime
import pytest
# TODO: generate
# , TimeSeriesResponse

TimeSeriesResponse = Any

@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def time_series_request(request: TimeSeriesRequest) -> TimeSeriesResponse:
    assert request.pentity_filters.pentity_name == request.pentity_aggregation.pentity_name
    array_str = lambda array: ",".join([str(x) for x in array])
    start_timestamp = datetime.utcfromtimestamp(request.request_info.start_timestamp.seconds).strftime('%Y-%m-%d %H:%M:%S')
    end_timestamp= datetime.utcfromtimestamp(request.request_info.end_timestamp.seconds).strftime('%Y-%m-%d %H:%M:%S')


    snql_query = f"""
MATCH ({request.pentity_filters.pentity_name})
SELECT {request.pentity_aggregation.aggregation_type}({request.pentity_aggregation.attribute_name})
BY start_timestamp
WHERE organization_id IN array({array_str(request.request_info.organization_ids)}) AND project_id IN array({array_str(request.request_info.project_ids)}) AND start_timestamp < toDateTime('{end_timestamp}') AND start_timestamp >= toDateTime('{start_timestamp}')
GRANULARITY {request.granularity_secs}
"""
    print("SNQL")
    print(snql_query)
    result = parse_and_run_query(
        body = {
            "query": snql_query,
            "tenant_ids": {"organization_id": request.request_info.organization_ids[0], "project_id": request.request_info.project_ids[0], "referrer": request.request_info.referrer},
            "referrer": request.request_info.referrer,
        },
        timer=Timer("timeseries"),
        is_mql=False
    )
    return result
