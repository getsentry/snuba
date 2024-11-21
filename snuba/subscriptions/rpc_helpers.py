import base64
from datetime import UTC, datetime, timedelta

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest

from snuba.reader import Result
from snuba.web import QueryResult
from snuba.web.rpc.v1.endpoint_time_series import EndpointTimeSeries


def build_rpc_request(
    timestamp: datetime,
    time_window_sec: int,
    time_series_request: str,
) -> TimeSeriesRequest:

    request_class = EndpointTimeSeries().request_class()()
    request_class.ParseFromString(base64.b64decode(time_series_request))

    # TODO: update it to round to the lowest granularity
    # rounded_ts = int(timestamp.replace(tzinfo=UTC).timestamp() / 15) * 15
    rounded_ts = (
        int(timestamp.replace(tzinfo=UTC).timestamp() / time_window_sec)
        * time_window_sec
    )
    rounded_start = datetime.utcfromtimestamp(rounded_ts)

    start_time_proto = Timestamp()
    start_time_proto.FromDatetime(rounded_start - timedelta(seconds=time_window_sec))
    end_time_proto = Timestamp()
    end_time_proto.FromDatetime(rounded_start)
    request_class.meta.start_timestamp.CopyFrom(start_time_proto)
    request_class.meta.end_timestamp.CopyFrom(end_time_proto)

    request_class.granularity_secs = time_window_sec

    return request_class


def run_rpc_subscription_query(
    request: TimeSeriesRequest,
) -> QueryResult:
    response = EndpointTimeSeries().execute(request)
    if not response.result_timeseries:
        result: Result = {
            "meta": [],
            "data": [{request.aggregations[0].label: 0}],
            "trace_output": "",
        }
        return QueryResult(
            result=result, extra={"stats": {}, "sql": "", "experiments": {}}
        )

    timeseries = response.result_timeseries[0]
    data = [{timeseries.label: timeseries.data_points[0].data}]

    result = {"meta": [], "data": data, "trace_output": ""}
    return QueryResult(result=result, extra={"stats": {}, "sql": "", "experiments": {}})
