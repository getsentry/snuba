import random
import struct
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, MutableMapping
from unittest.mock import patch

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1alpha.endpoint_aggregate_bucket_pb2 import (
    AggregateBucketRequest as AggregateBucketRequestProto,
)
from sentry_protos.snuba.v1alpha.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.common.eap_execute import run_eap_query
from snuba.web.rpc.v1alpha.timeseries.aggregate_functions import merge_t_digests_states
from snuba.web.rpc.v1alpha.timeseries.timeseries import AggregateBucketRequest
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events


def gen_message(dt: datetime, msg_index: int) -> MutableMapping[str, Any]:
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


BASE_TIME = datetime.utcnow().replace(
    hour=8, minute=0, second=0, microsecond=0, tzinfo=UTC
) - timedelta(hours=24)


@pytest.fixture(autouse=True)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    spans_storage = get_storage(StorageKey("eap_spans"))
    messages = [gen_message(BASE_TIME, i) for i in range(120)]

    # we also add some junk messages to make sure filtering works properly
    for junk_message in [gen_message(BASE_TIME, i) for i in range(120)]:
        junk_message["tags"] = {"hello": "world"}
        junk_message["measurements"] = {"blah": {"value": 50}}
        messages.append(junk_message)

    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTimeSeriesApi(BaseApiTest):
    def test_basic(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = AggregateBucketRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
            ),
            key=AttributeKey(name="project_id", type=AttributeKey.TYPE_INT),
            aggregate=AggregateBucketRequestProto.FUNCTION_SUM,
            granularity_secs=60,
        )
        response = self.app.post(
            "/rpc/AggregateBucketRequest/v1alpha", data=message.SerializeToString()
        )
        assert response.status_code == 200

    def test_sum(self, setup_teardown: Any) -> None:
        message = AggregateBucketRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + 60 * 30)),
            ),
            key=AttributeKey(name="eap.measurement", type=AttributeKey.TYPE_FLOAT),
            aggregate=AggregateBucketRequestProto.FUNCTION_SUM,
            granularity_secs=300,
        )
        response = AggregateBucketRequest().execute(message)
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

    def test_p99(self, setup_teardown: Any) -> None:
        message = AggregateBucketRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + 60 * 60)),
            ),
            key=AttributeKey(name="eap.measurement", type=AttributeKey.TYPE_FLOAT),
            aggregate=AggregateBucketRequestProto.FUNCTION_P99,
            granularity_secs=60 * 15,
        )
        response = AggregateBucketRequest().execute(message)
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
        assert response.result == expected_results

    def test_median(self, setup_teardown: Any) -> None:
        message = AggregateBucketRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + 60 * 60)),
            ),
            key=AttributeKey(name="eap.measurement", type=AttributeKey.TYPE_FLOAT),
            aggregate=AggregateBucketRequestProto.FUNCTION_P50,
            granularity_secs=60 * 20,
        )
        response = AggregateBucketRequest().execute(message)
        # spans have measurement = 0, 1, 2, ...
        # for us, starts at 60, and granularity puts 20 spans into each bucket
        # where the first and tenth of every 20 is 100x upscaled
        # so the P50 should be that the 10th, 30th, and 50th elements
        # (i.e., 70, 90, and 110)
        # T-Digest is approximate, so these numbers can be +- 3 or so.
        expected_results = pytest.approx(
            [
                60 + 20 * 0 + 10,
                60 + 20 * 1 + 10,
                60 + 20 * 2 + 10,
            ],
            rel=3,
        )
        assert response.result == expected_results

    def test_average(self, setup_teardown: Any) -> None:
        message = AggregateBucketRequestProto(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + 60 * 30)),
            ),
            key=AttributeKey(name="eap.measurement", type=AttributeKey.TYPE_FLOAT),
            aggregate=AggregateBucketRequestProto.FUNCTION_AVERAGE,
            granularity_secs=300,
        )
        response = AggregateBucketRequest().execute(message)
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

    @pytest.mark.parametrize(
        "states, level, expected_result",
        [
            pytest.param(
                # 8105 = 2 bytes varuint (ignored), then mean = 1, count = 1, mean = 5, count = 1
                # select quantileTDigest(0.5)(x) from numbers(1) array join [1, 5] as x;
                ["8105" + struct.pack("ffff", 1, 1, 5, 1).hex()],
                0.5,
                1,
                id="median of two numbers is smaller one",
            ),
            pytest.param(
                # ffffff05 = 4 bytes varuint (ignored), then mean = 1, count = 1, mean = 5, count = 1
                # select quantileTDigest(0.5)(x) from numbers(1) array join [1, 5, 5] as x;
                ["ffffff05" + struct.pack("ffffff", 1, 1, 5, 1, 5, 1).hex()],
                0.5,
                5,
                id="median of [1, 5, 5] = 5",
            ),
            pytest.param(
                # 05 = 1 byte varuint (ignored), then mean = 5, count = 1, mean = 100, count = 1
                ["05" + struct.pack("f" * 198, *[1, 1] * 98 + [100, 1]).hex()],
                0.99,
                100,
                id="p99 of [1] * 98 + [100] = 100",
            ),
            pytest.param(
                # 05 = 1 byte varuint (ignored), then mean = 5, count = 1, mean = 100, count = 1
                ["05" + struct.pack("ffff", 1, 199, 100, 1).hex()],
                0.99,
                99,
                # this is a test of interpolation, the centroids here are not ones you'd normally get from T-digest
                id="p99 of [1] * 199 + [100] * 1 = 99",
            ),
        ],
    )
    def test_merge_t_digests_states(
        self, states: list[str], level: float, expected_result: float
    ) -> None:
        assert merge_t_digests_states(states, level) == pytest.approx(
            expected_result, rel=0.01
        )

    @pytest.mark.parametrize(
        "aggregate, expected_result",
        [
            (
                AggregateBucketRequestProto.FUNCTION_COUNT,
                [
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    sum((100 if x % 10 == 0 else 1) for x in range(120)),
                ],
            ),
            (
                AggregateBucketRequestProto.FUNCTION_AVERAGE,
                [
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    pytest.approx(
                        sum(x * (100 if x % 10 == 0 else 1) for x in range(120))
                        / sum((100 if x % 10 == 0 else 1) for x in range(120))
                    ),
                ],
            ),
            (
                AggregateBucketRequestProto.FUNCTION_SUM,
                [
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    sum(x * (100 if x % 10 == 0 else 1) for x in range(120)),
                ],
            ),
            (
                AggregateBucketRequestProto.FUNCTION_P50,
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, pytest.approx(59.5, rel=0.01)],
            ),
            (
                AggregateBucketRequestProto.FUNCTION_P95,
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, pytest.approx(110, rel=0.01)],
            ),
            (
                AggregateBucketRequestProto.FUNCTION_P99,
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, pytest.approx(110, rel=0.01)],
            ),
        ],
    )
    def test_query_caching(
        self,
        setup_teardown: Any,
        aggregate: AggregateBucketRequestProto.Function.ValueType,
        expected_result: list[float],
    ) -> None:
        with patch(
            "snuba.web.rpc.common.eap_execute.run_eap_query",
            side_effect=run_eap_query,
        ) as mocked_run_query:
            # this test does a daily aggregate on a week + 9 hours of data.
            # that is, the user is visiting the page at 09:01 GMT, so the 8 buckets returned would be:
            # 00:00-23:99 7 days ago,
            # 00:00-23:99 6 days ago,
            # (etc)
            # 00:00-23:99 yesterday
            # 00:00-09:00 today

            base_timestamp = int(BASE_TIME.replace(hour=0).timestamp())
            # due to our caching logic, each bucket will be split into three smaller requests from 0-8, 8-16, 16-24
            # we only have data for the 00:00-23:99 yesterday and 00:00-00:01 buckets, the rest are all 0
            message = AggregateBucketRequestProto(
                meta=RequestMeta(
                    project_ids=[1, 2, 3],
                    organization_id=1,
                    cogs_category="something",
                    referrer="something",
                    start_timestamp=Timestamp(
                        seconds=base_timestamp - 60 * 60 * 24 * 7
                    ),
                    end_timestamp=Timestamp(seconds=base_timestamp + 60 * 60 * 9),
                ),
                key=AttributeKey(name="eap.measurement", type=AttributeKey.TYPE_FLOAT),
                aggregate=aggregate,
                granularity_secs=60 * 60 * 24,
            )
            response = AggregateBucketRequest().execute(message)

            assert (
                mocked_run_query.call_count == 3 * 7 + 2
            )  # 3 8-hour buckets/day + 0:00-8:00 + 8:00-9:00

            # we expect all buckets except the last one to be cached
            assert list(
                call[1]["clickhouse_settings"]["use_query_cache"]
                for call in mocked_run_query.call_args_list[:-1]
            ) == ["true"] * (3 * 7 + 1)

            # we expect all buckets >4 hours old to have a long TTL
            assert list(
                call[1]["clickhouse_settings"]["query_cache_ttl"]
                for call in mocked_run_query.call_args_list[:-2]
            ) == [90 * 24 * 60 * 60] * (3 * 7)

            assert response.result == expected_result
