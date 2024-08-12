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

import pytest

@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTimeSeriesApi(BaseApiTest):
    def test_basic(self):
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TimeSeriesRequest(
            request_info=RequestInfo(
                project_ids=[1, 2, 3],
                organization_ids=[1],
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
            ),
            pentity_filters=PentityFilters(
                pentity_name="eap_spans", filters=[PentityFilter(attribute_name="op", comparison="=", string_literal="get")]
            ),
            pentity_aggregation=PentityAggregation(aggregation_type="p90", pentity_name="eap_spans", attribute_name="duration_ms"),
            granularity_secs=60
        )
        response = self.app.post("/timeseries", data=message.SerializeToString())
        assert response.status == 200
