import random
import uuid
from datetime import datetime, timedelta
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1alpha.endpoint_aggregate_bucket_pb2 import (
    AggregateBucketRequest,
)
from sentry_protos.snuba.v1alpha.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.timeseries.timeseries import timeseries_query
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events


def gen_message(dt: datetime, msg_index: int) -> Mapping[str, Any]:
    dt = dt - timedelta(hours=1) + timedelta(minutes=msg_index)
    return {
        "description": "/api/0/relays/projectconfigs/",
        "duration_ms": 152,
        "event_id": "d826225de75d42d6b2f01b957d51f18f",
        "exclusive_time_ms": 0.228,
        "is_segment": True,
        "data": {
            "sentry.environment": "development",
            "sentry.release": "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b",
            "thread.name": "uWSGIWorker1Core0",
            "thread.id": "8522009600",
            "sentry.segment.name": "/api/0/relays/projectconfigs/",
            "sentry.sdk.name": "sentry.python.django",
            "sentry.sdk.version": "2.7.0",
            "my.float.field": 101.2,
            "my.int.field": 2000,
            "my.neg.field": -100,
            "my.neg.float.field": -101.2,
            "my.true.bool.field": True,
            "my.false.bool.field": False,
        },
        "measurements": {
            "num_of_spans": {"value": 50.0},
            "eap.measurement": {"value": msg_index},
            "client_sample_rate": {
                "value": 0.01
                if msg_index % 10 == 0
                else 1  # every 10th span should be 100x upscaled
            },
        },
        "organization_id": 1,
        "origin": "auto.http.django",
        "project_id": 1,
        "received": 1721319572.877828,
        "retention_days": 90,
        "segment_id": "8873a98879faf06d",
        "sentry_tags": {
            "category": "http",
            "environment": "development",
            "op": "http.server",
            "platform": "python",
            "release": "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b",
            "sdk.name": "sentry.python.django",
            "sdk.version": "2.7.0",
            "status": "ok",
            "status_code": "200",
            "thread.id": "8522009600",
            "thread.name": "uWSGIWorker1Core0",
            "trace.status": "ok",
            "transaction": "/api/0/relays/projectconfigs/",
            "transaction.method": "POST",
            "transaction.op": "http.server",
            "user": "ip:127.0.0.1",
        },
        "span_id": uuid.uuid4().hex,
        "tags": {
            "http.status_code": "200",
            "relay_endpoint_version": "3",
            "relay_id": "88888888-4444-4444-8444-cccccccccccc",
            "relay_no_cache": "False",
            "relay_protocol_version": "3",
            "relay_use_post_or_schedule": "True",
            "relay_use_post_or_schedule_rejected": "version",
            "server_name": "D23CXQ4GK2.local",
            "spans_over_limit": "False",
            "color": random.choice(["red", "green", "blue"]),
            "location": random.choice(["mobile", "frontend", "backend"]),
        },
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(dt.timestamp()) * 1000,
        "start_timestamp_precise": dt.timestamp(),
        "end_timestamp_precise": dt.timestamp() + 1,
    }


BASE_TIME = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
    minutes=180
)


@pytest.fixture(autouse=True)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    spans_storage = get_storage(StorageKey("eap_spans"))
    messages = [gen_message(BASE_TIME, i) for i in range(120)]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTimeSeriesApi(BaseApiTest):
    def test_basic(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = AggregateBucketRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
            ),
            key=AttributeKey(name="project_id", type=AttributeKey.TYPE_INT),
            aggregate=AggregateBucketRequest.FUNCTION_SUM,
            granularity_secs=60,
        )
        response = self.app.post(
            "/rpc/AggregateBucketRequest", data=message.SerializeToString()
        )
        assert response.status_code == 200

    def test_sum(self, setup_teardown: Any) -> None:
        message = AggregateBucketRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + 60 * 30)),
            ),
            key=AttributeKey(name="eap.measurement", type=AttributeKey.TYPE_FLOAT),
            aggregate=AggregateBucketRequest.FUNCTION_SUM,
            granularity_secs=300,
        )
        response = timeseries_query(message)
        # spans have (measurement, sample rate) = (0, 100), (10, 1), ..., (100, 100)
        # granularity puts five spans into the same bucket
        # whole interval is 30 minutes, so there should be 6 buckets
        # and our start time is exactly 1 hour after data stops
        expected_results = [
            60 * 100 + 61 + 62 + 63 + 64,
            65 + 66 + 67 + 68 + 69,
            70 * 100 + 71 + 72 + 73 + 74,
            75 + 76 + 77 + 78 + 79,
            80 * 100 + 81 + 82 + 83 + 84,
            85 + 86 + 87 + 88 + 89,
        ]
        assert response.result == expected_results

    def test_quantiles(self, setup_teardown: Any) -> None:
        message = AggregateBucketRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + 60 * 60)),
            ),
            key=AttributeKey(name="eap.measurement", type=AttributeKey.TYPE_FLOAT),
            aggregate=AggregateBucketRequest.FUNCTION_P99,
            granularity_secs=60 * 15,
        )
        response = timeseries_query(message)
        # spans have measurement = 0, 1, 2, ...
        # for us, starts at 60, and granularity puts 15 spans into each bucket
        # and the P99 of 15 spans is just the maximum of the 15.
        # T-Digest is approximate, so these numbers can be +- 3 or so.
        expected_results = pytest.approx(
            [
                60 + 15 * 0 + 14,
                60 + 15 * 1 + 14,
                60 + 15 * 2 + 14,
                60 + 15 * 3 + 14,
            ],
            rel=3,
        )
        print(response.result)
        print(expected_results)
        assert response.result == expected_results

    def test_average(self, setup_teardown: Any) -> None:
        message = AggregateBucketRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + 60 * 30)),
            ),
            key=AttributeKey(name="eap.measurement", type=AttributeKey.TYPE_FLOAT),
            aggregate=AggregateBucketRequest.FUNCTION_AVERAGE,
            granularity_secs=300,
        )
        response = timeseries_query(message)
        # spans have (measurement, sample rate) = (0, 100), (10, 1), ..., (100, 100)
        # granularity puts five spans into the same bucket
        # whole interval is 30 minutes, so there should be 6 buckets
        # and our start time is exactly 1 hour after data stops
        expected_results = pytest.approx(
            [
                (60 * 100 + 61 + 62 + 63 + 64) / 104,
                (65 + 66 + 67 + 68 + 69) / 5,
                (70 * 100 + 71 + 72 + 73 + 74) / 104,
                (75 + 76 + 77 + 78 + 79) / 5,
                (80 * 100 + 81 + 82 + 83 + 84) / 104,
                (85 + 86 + 87 + 88 + 89) / 5,
            ]
        )
        assert response.result == expected_results
