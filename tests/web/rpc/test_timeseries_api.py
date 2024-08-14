import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from snuba.protobufs import AggregateBucket_pb2
from snuba.protobufs.BaseRequest_pb2 import RequestInfo
from tests.base import BaseApiTest


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTimeSeriesApi(BaseApiTest):
    def test_basic(self):
        ts = Timestamp()
        ts.GetCurrentTime()
        message = AggregateBucket_pb2.AggregateBucketRequest(
            request_info=RequestInfo(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
            ),
            granularity_secs=60,
        )
        response = self.app.post("/timeseries", data=message.SerializeToString())
        assert response.status_code == 200

        # STUB response test
        pbuf_response = AggregateBucket_pb2.AggregateBucketResponse()
        pbuf_response.ParseFromString(response.data)

        assert pbuf_response.result == [float(i) for i in range(100)]
