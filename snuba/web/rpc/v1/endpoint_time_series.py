from typing import Type
from snuba.web.rpc import RPCEndpoint
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest, TimeSeriesResponse, DataPoint
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta




class EndpointTimeSeries(RPCEndpoint[TimeSeriesRequest, TimeSeriesResponse]):

    @classmethod
    def version(cls):
        return "v1"

    @classmethod
    def request_class(cls) -> Type[TimeSeriesRequest]:
        return TimeSeriesRequest

    @classmethod
    def response_class(cls) -> Type[TimeSeriesResponse]:
        return TimeSeriesResponse

    def _execute(self, in_msg: TimeSeriesRequest) -> TimeSeriesResponse:
        raise NotImplementedError
