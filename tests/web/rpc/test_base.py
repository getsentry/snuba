import time
from typing import Type

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from snuba.web.rpc import RPCEndpoint, list_all_endpoint_names
from tests.backends.metrics import TestingMetricsBackend


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
    assert metric_names_to_metric["rpc.endpoint_timing"].value == pytest.approx(MyRPC.duration_millis, rel=10)  # type: ignore
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
