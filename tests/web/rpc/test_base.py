import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping, Type
from unittest.mock import patch

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
    with patch("snuba.web.rpc.sentry_sdk.capture_exception") as sentry_sdk_mock:
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
        sentry_sdk_mock.assert_called()


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


def gen_message(
    dt: datetime,
) -> Mapping[str, Any]:
    return {
        "description": "/api/0/relays/projectconfigs/",
        "duration_ms": 152,
        "event_id": "d826225de75d42d6b2f01b957d51f18f",
        "exclusive_time_ms": 0.228,
        "is_segment": True,
        "organization_id": 1,
        "origin": "auto.http.django",
        "project_id": 1,
        "received": 1721319572.877828,
        "retention_days": 90,
        "segment_id": "8873a98879faf06d",
        "span_id": "123456781234567D",
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(dt.timestamp() * 1000),
        "start_timestamp_precise": dt.timestamp(),
        "end_timestamp_precise": dt.timestamp() + 1,
    }


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_trim_time_range(snuba_set_config) -> None:
    snuba_set_config("use_eap_items_table", True)
    snuba_set_config("use_eap_items_table_start_timestamp_seconds", 0)

    spans_storage = get_storage(StorageKey("eap_items"))
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


_REFERRER = "something"


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
@pytest.mark.parametrize(
    "hours, expected_time_bucket",
    [
        (0.5, "lte_1_hour"),
        (1, "lte_1_hour"),
        (24, "lte_1_day"),
        (3 * 24, "lte_7_days"),
        (11 * 24, "lte_14_days"),
        (22 * 24, "lte_30_days"),
        (90 * 24, "lte_90_days"),
        (99 * 24, "gt_90_days"),
    ],
)
def test_tagged_metrics(
    hours: int, expected_time_bucket: str, snuba_set_config
) -> None:
    snuba_set_config("use_eap_items_table", True)
    snuba_set_config("use_eap_items_table_start_timestamp_seconds", 0)

    end_timestamp = Timestamp(seconds=int(_BASE_TIME.timestamp()))
    start_timestamp = Timestamp(
        seconds=int((_BASE_TIME - timedelta(hours=hours)).timestamp())
    )
    message = TraceItemTableRequest(
        meta=RequestMeta(
            project_ids=[1],
            organization_id=1,
            cogs_category="something",
            referrer=_REFERRER,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
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
    metrics_backend = TestingMetricsBackend()
    EndpointTraceItemTable(metrics_backend=metrics_backend).execute(message)
    metric_tags = [m.tags for m in metrics_backend.calls]
    assert metric_tags == [
        {
            "endpoint_name": "EndpointTraceItemTable",
            "version": "v1",
            "time_period": expected_time_bucket,
            "referrer": _REFERRER,
        }
        for _ in range(len(metrics_backend.calls))
    ]
