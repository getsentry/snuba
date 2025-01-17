import math
import random
import uuid
from datetime import datetime, timedelta
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    ExtrapolationMode,
    Function,
    Reliability,
)

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events

_RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"
_SERVER_NAME = "D23CXQ4GK2.local"


def gen_message(
    dt: datetime,
    measurements: dict[str, dict[str, float]] | None = None,
    tags: dict[str, str] | None = None,
) -> Mapping[str, Any]:
    measurements = measurements or {}
    tags = tags or {}
    return {
        "description": "/api/0/relays/projectconfigs/",
        "duration_ms": 152,
        "event_id": "d826225de75d42d6b2f01b957d51f18f",
        "exclusive_time_ms": 0.228,
        "is_segment": True,
        "data": {
            "sentry.environment": "development",
            "sentry.release": _RELEASE_TAG,
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
            "eap.measurement": {"value": random.choice([1, 100, 1000])},
            **measurements,
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
            "release": _RELEASE_TAG,
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
        "span_id": "123456781234567D",
        "tags": {
            "http.status_code": "200",
            "relay_endpoint_version": "3",
            "relay_id": "88888888-4444-4444-8444-cccccccccccc",
            "relay_no_cache": "False",
            "relay_protocol_version": "3",
            "relay_use_post_or_schedule": "True",
            "relay_use_post_or_schedule_rejected": "version",
            "server_name": _SERVER_NAME,
            "spans_over_limit": "False",
            "color": random.choice(["red", "green", "blue"]),
            "location": random.choice(["mobile", "frontend", "backend"]),
            **tags,
        },
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(dt.timestamp()) * 1000 - int(random.gauss(1000, 200)),
        "start_timestamp_precise": dt.timestamp(),
        "end_timestamp_precise": dt.timestamp() + 1,
    }


BASE_TIME = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
    minutes=180
)


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemTableWithExtrapolation(BaseApiTest):
    def test_aggregation_on_attribute_column(self) -> None:
        spans_storage = get_storage(StorageKey("eap_spans"))
        start = BASE_TIME
        tags = {"custom_tag": "blah"}
        messages_w_measurement = [
            gen_message(
                start - timedelta(minutes=i),
                measurements={
                    "custom_measurement": {
                        "value": i
                    },  # this results in values of 0, 1, 2, 3, and 4
                    "server_sample_rate": {
                        "value": 1.0 / (2**i)
                    },  # this results in sampling weights of 1, 2, 4, 8, and 16
                },
                tags=tags,
            )
            for i in range(5)
        ]
        messages_no_measurement = [
            gen_message(start - timedelta(minutes=i), tags=tags) for i in range(5)
        ]
        write_raw_unprocessed_events(spans_storage, messages_w_measurement + messages_no_measurement)  # type: ignore

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="sum(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="avg(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_INT, name="custom_measurement"
                        ),
                        label="count(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    ),
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="sentry.duration_ms"
                        ),
                        label="count(sentry.duration_ms)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    ),
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_P90,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="p90(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    ),
                ),
            ],
            order_by=[],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        measurement_sum = [v.val_double for v in response.column_values[0].results][0]
        measurement_avg = [v.val_double for v in response.column_values[1].results][0]
        measurement_count_custom_measurement = [
            v.val_double for v in response.column_values[2].results
        ][0]
        measurement_count_duration = [
            v.val_double for v in response.column_values[3].results
        ][0]
        measurement_p90 = [v.val_double for v in response.column_values[4].results][0]
        assert measurement_sum == 98  # weighted sum - 0*1 + 1*2 + 2*4 + 3*8 + 4*16
        assert (
            abs(measurement_avg - 3.16129032) < 0.000001
        )  # weighted average - (0*1 + 1*2 + 2*4 + 3*8 + 4*16) / (1+2+4+8+16)
        assert (
            measurement_count_custom_measurement == 31
        )  # weighted count - 1 + 2 + 4 + 8 + 16
        assert (
            measurement_count_duration == 36
        )  # weighted count (all events have duration) - 5*1 + 1 + 2 + 4 + 8 + 16
        assert abs(measurement_p90 - 4) < 0.01  # weighted p90 - 4

    def test_count_reliability(self) -> None:
        spans_storage = get_storage(StorageKey("eap_spans"))
        start = BASE_TIME
        tags = {"custom_tag": "blah"}
        messages_w_measurement = [
            gen_message(
                start - timedelta(minutes=i),
                measurements={
                    "custom_measurement": {
                        "value": i
                    },  # this results in values of 0, 1, 2, 3, and 4
                    "server_sample_rate": {"value": 1.0},
                },
                tags=tags,
            )
            for i in range(5)
        ]
        messages_no_measurement = [
            gen_message(start - timedelta(minutes=i), tags=tags) for i in range(5)
        ]
        write_raw_unprocessed_events(spans_storage, messages_w_measurement + messages_no_measurement)  # type: ignore

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="count(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
            ],
            order_by=[],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        measurement_count = [v.val_double for v in response.column_values[0].results][0]
        measurement_reliability = [v for v in response.column_values[0].reliabilities][
            0
        ]
        assert measurement_count == 5
        assert (
            measurement_reliability == Reliability.RELIABILITY_LOW
        )  # low reliability due to low sample count

    def test_count_reliability_with_group_by(self) -> None:
        spans_storage = get_storage(StorageKey("eap_spans"))
        start = BASE_TIME
        messages_w_measurement = [
            gen_message(
                start - timedelta(minutes=i),
                measurements={
                    "custom_measurement": {
                        "value": i
                    },  # this results in values of 0, 1, 2, 3, and 4
                    "server_sample_rate": {"value": 1.0},
                },
                tags={"key": "foo"},
            )
            for i in range(5)
        ]
        messages_no_measurement = [
            gen_message(start - timedelta(minutes=i), tags={"key": "bar"})
            for i in range(5)
        ]
        write_raw_unprocessed_events(spans_storage, messages_w_measurement + messages_no_measurement)  # type: ignore

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="key")),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="sum(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="avg(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="count(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_P90,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="p90(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    )
                ),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="key")
                    ),
                    descending=True,
                ),
            ],
            group_by=[
                AttributeKey(type=AttributeKey.TYPE_STRING, name="key"),
            ],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)

        measurement_tags = [v.val_str for v in response.column_values[0].results]
        assert measurement_tags == ["foo", "bar"]

        measurement_sums = [v.val_double for v in response.column_values[1].results]
        measurement_reliabilities = [v for v in response.column_values[1].reliabilities]
        assert measurement_sums == [sum(range(5)), 0]
        assert measurement_reliabilities == [
            Reliability.RELIABILITY_LOW,
            Reliability.RELIABILITY_UNSPECIFIED,
        ]  # low reliability due to low sample count

        measurement_avgs = [v.val_double for v in response.column_values[2].results]
        measurement_reliabilities = [v for v in response.column_values[2].reliabilities]
        assert len(measurement_avgs) == 2
        assert measurement_avgs[0] == sum(range(5)) / 5
        assert math.isnan(measurement_avgs[1])
        assert measurement_reliabilities == [
            Reliability.RELIABILITY_LOW,
            Reliability.RELIABILITY_UNSPECIFIED,
        ]  # low reliability due to low sample count

        measurement_counts = [v.val_double for v in response.column_values[3].results]
        measurement_reliabilities = [v for v in response.column_values[3].reliabilities]
        assert measurement_counts == [5, 0]
        assert measurement_reliabilities == [
            Reliability.RELIABILITY_LOW,
            Reliability.RELIABILITY_UNSPECIFIED,
        ]  # low reliability due to low sample count

        measurement_p90s = [v.val_double for v in response.column_values[4].results]
        measurement_reliabilities = [v for v in response.column_values[4].reliabilities]
        assert len(measurement_p90s) == 2
        assert measurement_p90s[0] == 4
        assert math.isnan(measurement_p90s[1])
        assert measurement_reliabilities == [
            Reliability.RELIABILITY_LOW,
            Reliability.RELIABILITY_UNSPECIFIED,
        ]  # low reliability due to low sample count
