import time
import uuid
from datetime import timedelta
from typing import Type
from unittest.mock import patch

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.web import QueryException
from snuba.web.rpc import RPCEndpoint, list_all_endpoint_names
from snuba.web.rpc.common.exceptions import (
    HighAccuracyQueryTimeoutException,
    QueryTimeoutException,
)
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.backends.metrics import TestingMetricsBackend
from tests.web.rpc.v1.test_utils import BASE_TIME

RANDOM_REQUEST_ID = str(uuid.uuid4())


def _get_in_msg() -> TimeSeriesRequest:
    ts = Timestamp()
    ts.GetCurrentTime()
    tstart = Timestamp(seconds=ts.seconds - 3600)

    return TimeSeriesRequest(
        meta=RequestMeta(
            request_id=RANDOM_REQUEST_ID,
            project_ids=[1, 2, 3],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            start_timestamp=tstart,
            end_timestamp=ts,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        ),
        granularity_secs=60,
    )


class RPCException(Exception):
    pass


class MyRPC(RPCEndpoint[TimeSeriesRequest, TimeSeriesRequest]):
    duration_millis = 100

    @classmethod
    def version(cls) -> str:
        return "v1"

    def _execute(self, in_msg: TimeSeriesRequest) -> TimeSeriesRequest:
        time.sleep(self.duration_millis / 1000)
        return in_msg


class ErrorRPC(RPCEndpoint[TimeSeriesRequest, TimeSeriesRequest]):
    duration_millis = 100

    @classmethod
    def response_class(cls) -> Type[TimeSeriesRequest]:
        return TimeSeriesRequest

    @classmethod
    def version(cls) -> str:
        return "v1"

    def _execute(self, in_msg: TimeSeriesRequest) -> TimeSeriesRequest:
        time.sleep(self.duration_millis / 1000)
        raise RPCException("This is meant to error!")


class TimeoutRPC(RPCEndpoint[TimeSeriesRequest, TimeSeriesRequest]):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def response_class(cls) -> Type[TimeSeriesRequest]:
        return TimeSeriesRequest

    def _execute(self, in_msg: TimeSeriesRequest) -> TimeSeriesRequest:
        raise QueryException("timed out", extra={"stats": {"error_code": 159}})


def test_endpoint_name_resolution() -> None:
    assert RPCEndpoint.get_from_name("MyRPC", "v1") is MyRPC


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_before_and_after_execute() -> None:
    before_called = False
    after_called = False

    class BeforeAndAfter(RPCEndpoint[TimeSeriesRequest, TimeSeriesRequest]):
        @classmethod
        def version(cls) -> str:
            return "v1"

        def _before_execute(self, in_msg: TimeSeriesRequest) -> None:
            nonlocal before_called
            before_called = True

        def _execute(self, in_msg: TimeSeriesRequest) -> TimeSeriesRequest:
            return in_msg

        def _after_execute(
            self,
            in_msg: TimeSeriesRequest,
            out_msg: TimeSeriesRequest,
            error: Exception | None,
        ) -> TimeSeriesRequest:
            nonlocal after_called
            after_called = True
            return out_msg

    BeforeAndAfter().execute(_get_in_msg())
    assert before_called
    assert after_called


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_metrics() -> None:
    metrics_backend = TestingMetricsBackend()
    rpc_call = MyRPC(metrics_backend=metrics_backend)
    rpc_call.execute(_get_in_msg())
    metric_tags = [m.tags for m in metrics_backend.calls]
    assert metric_tags == [
        {
            "time_period": "lte_1_day",
            "referrer": "something",
            "endpoint_name": "MyRPC",
            "version": "v1",
        }
        for _ in range(len(metrics_backend.calls))
    ]

    metric_names_to_metric = {m.name: m for m in metrics_backend.calls}  # type: ignore
    assert metric_names_to_metric["rpc.endpoint_timing"].value == pytest.approx(  # type: ignore
        MyRPC.duration_millis, rel=10
    )
    assert metric_names_to_metric["rpc.request_success"].value == 1  # type: ignore


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_error_metrics() -> None:
    with patch("snuba.web.rpc.sentry_sdk.capture_exception") as sentry_sdk_mock:
        metrics_backend = TestingMetricsBackend()
        rpc_call = ErrorRPC(metrics_backend=metrics_backend)
        with pytest.raises(RPCException):
            rpc_call.execute(_get_in_msg())
        metric_tags = [m.tags for m in metrics_backend.calls]
        # the last tags set only contains endpoint_name and version because in __after_execute's metrics_backend.increment, we don't pass in the other tags
        assert metric_tags == [
            {
                "time_period": "lte_1_day",
                "referrer": "something",
                "endpoint_name": "ErrorRPC",
                "version": "v1",
            }
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
def test_tagged_metrics(hours: int, expected_time_bucket: str) -> None:
    end_timestamp = Timestamp(seconds=int(BASE_TIME.timestamp()))
    start_timestamp = Timestamp(
        seconds=int((BASE_TIME - timedelta(hours=hours)).timestamp())
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


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_timeout_errors() -> None:
    ts = Timestamp()
    ts.GetCurrentTime()
    message = TimeSeriesRequest(
        meta=RequestMeta(
            project_ids=[1, 2, 3],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            start_timestamp=ts,
            end_timestamp=ts,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            downsampled_storage_config=DownsampledStorageConfig(
                mode=DownsampledStorageConfig.MODE_NORMAL
            ),
        ),
    )
    with pytest.raises(QueryTimeoutException):
        TimeoutRPC().execute(message)

    message = TimeSeriesRequest(
        meta=RequestMeta(
            project_ids=[1, 2, 3],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            start_timestamp=ts,
            end_timestamp=ts,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            downsampled_storage_config=DownsampledStorageConfig(
                mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY
            ),
        ),
    )
    with pytest.raises(HighAccuracyQueryTimeoutException):
        TimeoutRPC().execute(message)
