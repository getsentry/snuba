import random
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping, Type

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc import (
    MAXIMUM_TIME_RANGE_IN_DAYS,
    RPCEndpoint,
    list_all_endpoint_names,
)
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.backends.metrics import TestingMetricsBackend
from tests.helpers import write_raw_unprocessed_events


class RPCException(Exception):
    pass


class MyRPC(RPCEndpoint[Timestamp, Timestamp]):
    duration_millis = 100

    @classmethod
    def version(cls) -> str:
        return "v1"

    def _execute(self, in_msg: Timestamp) -> Timestamp:
        time.sleep(self.duration_millis / 1000)
        return Timestamp()


class ErrorRPC(RPCEndpoint[Timestamp, Timestamp]):
    duration_millis = 100

    @classmethod
    def response_class(cls) -> Type[Timestamp]:
        return Timestamp

    @classmethod
    def version(cls) -> str:
        return "v1"

    def _execute(self, in_msg: Timestamp) -> Timestamp:
        time.sleep(self.duration_millis / 1000)
        raise RPCException("This is meant to error!")


def test_endpoint_name_resolution() -> None:
    assert RPCEndpoint.get_from_name("MyRPC", "v1") is MyRPC


def test_before_and_after_execute() -> None:
    before_called = False
    after_called = False

    class BeforeAndAfter(RPCEndpoint[Timestamp, Timestamp]):
        @classmethod
        def version(cls) -> str:
            return "v1"

        def _before_execute(self, in_msg: Timestamp) -> None:
            nonlocal before_called
            before_called = True

        def _execute(self, in_msg: Timestamp) -> Timestamp:
            return in_msg

        def _after_execute(
            self, in_msg: Timestamp, out_msg: Timestamp, error: Exception | None
        ) -> Timestamp:
            nonlocal after_called
            after_called = True
            return out_msg

    BeforeAndAfter().execute(Timestamp())
    assert before_called
    assert after_called


def test_metrics() -> None:
    metrics_backend = TestingMetricsBackend()
    rpc_call = MyRPC(metrics_backend=metrics_backend)
    rpc_call.execute(Timestamp())
    metric_tags = [m.tags for m in metrics_backend.calls]
    assert metric_tags == [
        {"endpoint_name": "MyRPC", "version": "v1"}
        for _ in range(len(metrics_backend.calls))
    ]

    metric_names_to_metric = {m.name: m for m in metrics_backend.calls}  # type: ignore
    assert metric_names_to_metric["rpc.endpoint_timing"].value == pytest.approx(  # type: ignore
        MyRPC.duration_millis, rel=10
    )
    assert metric_names_to_metric["rpc.request_success"].value == 1  # type: ignore


def test_error_metrics() -> None:
    metrics_backend = TestingMetricsBackend()
    rpc_call = ErrorRPC(metrics_backend=metrics_backend)
    with pytest.raises(RPCException):
        rpc_call.execute(Timestamp())
    metric_tags = [m.tags for m in metrics_backend.calls]
    assert metric_tags == [
        {"endpoint_name": "ErrorRPC", "version": "v1"}
        for _ in range(len(metrics_backend.calls))
    ]

    metric_names_to_metric = {m.name: m for m in metrics_backend.calls}  # type: ignore
    assert metric_names_to_metric["rpc.request_error"].value == 1  # type: ignore


def test_list_all_endpoint_names() -> None:
    endpoint_names = list_all_endpoint_names()
    assert isinstance(endpoint_names, list)
    assert ("MyRPC", "v1") in endpoint_names
    assert ("ErrorRPC", "v1") in endpoint_names


_BASE_TIME = datetime.now(tz=UTC).replace(
    minute=0,
    second=0,
    microsecond=0,
)
_REQUEST_ID = uuid.uuid4().hex
_RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"
_SERVER_NAME = "D23CXQ4GK2.local"


def gen_message(
    dt: datetime,
    measurements: dict[str, dict[str, float]] | None = None,
    tags: dict[str, str] | None = None,
) -> Mapping[str, Any]:
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
        },
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(dt.timestamp()) * 1000 - int(random.gauss(1000, 200)),
        "start_timestamp_precise": dt.timestamp(),
        "end_timestamp_precise": dt.timestamp() + 1,
    }


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_trim_time_range() -> None:
    spans_storage = get_storage(StorageKey("eap_spans"))
    write_raw_unprocessed_events(
        spans_storage,  # type: ignore
        [
            gen_message(
                dt=_BASE_TIME - timedelta(days=i),
            )
            for i in range(90)
        ],
    )
    ts = Timestamp(seconds=int(_BASE_TIME.timestamp()))
    ninety_days_before = Timestamp(
        seconds=int((_BASE_TIME - timedelta(days=90)).timestamp())
    )
    message = TraceItemTableRequest(
        meta=RequestMeta(
            project_ids=[1],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            start_timestamp=ninety_days_before,
            end_timestamp=ts,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        ),
        columns=[
            Column(
                key=AttributeKey(
                    type=AttributeKey.TYPE_DOUBLE,
                    name="sentry.duration_ms",
                ),
            )
        ],
    )
    response = EndpointTraceItemTable().execute(message)
    assert len(response.column_values[0].results) == MAXIMUM_TIME_RANGE_IN_DAYS
