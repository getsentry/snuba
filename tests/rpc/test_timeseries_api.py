import time

from google.protobuf.timestamp_pb2 import Timestamp

from snuba.protobufs.base_messages_pb2 import (
    PentityAggregation,
    PentityFilter,
    PentityFilters,
    RequestInfo,
)
from snuba.protobufs.time_series_pb2 import TimeSeriesRequest
from tests.base import BaseApiTest


class TestTimeSeriesApi(BaseApiTest):
    def test_basic(self):
        message = TimeSeriesRequest(
            request_info=RequestInfo(
                project_ids=[1, 2, 3],
                organization_ids=[1],
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp().GetCurrentTime(),
                end_timestamp=Timestamp().GetCurrentTime(),
            ),
            pentity_filters=PentityFilters(
                pentity_name="spans", filters=[PentityFilter("op", "=", "get")]
            ),
            pentity_aggregation=PentityAggregation("p90", "spans", "duration"),
        )
        self.app.post("/timeseries", data=message.SerializeToString())
