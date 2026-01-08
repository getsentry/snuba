import uuid
from datetime import timedelta
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_details_pb2 import (
    TraceItemDetailsRequest,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_details import (
    EndpointTraceItemDetails,
    _convert_results,
)
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import (
    BASE_TIME,
    END_TIMESTAMP,
    START_TIMESTAMP,
    gen_item_message,
)

_REQUEST_ID = uuid.uuid4().hex
_TRACE_ID = str(uuid.uuid4())


@pytest.fixture(autouse=False)
def setup_logs_in_db(clickhouse_db: None, redis_db: None) -> None:
    logs_storage = get_storage(StorageKey("eap_items"))
    messages = []
    for i in range(120):
        timestamp = BASE_TIME + timedelta(minutes=i)
        timestamp_nanos = int(timestamp.timestamp() * 1e9)
        messages.append(
            gen_item_message(
                start_timestamp=timestamp,
                remove_default_attributes=True,
                type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                attributes={
                    "bool_tag": AnyValue(bool_value=i % 2 == 0),
                    "double_tag": AnyValue(double_value=1234567890.123),
                    "int_tag": AnyValue(int_value=i),
                    "observed_timestamp_nanos": AnyValue(int_value=timestamp_nanos),
                    "sentry.body": AnyValue(string_value=f"hello world {i}"),
                    "sentry.severity_number": AnyValue(int_value=10),
                    "sentry.severity_text": AnyValue(string_value="info"),
                    "sentry.timestamp_precise": AnyValue(int_value=timestamp_nanos),
                    "span_id": AnyValue(string_value="123456781234567D"),
                    "str_tag": AnyValue(string_value=f"num: {i}"),
                    "timestamp_nanos": AnyValue(int_value=timestamp_nanos),
                },
            )
        )
    write_raw_unprocessed_events(logs_storage, messages)  # type: ignore


@pytest.fixture(autouse=False)
def setup_spans_in_db(clickhouse_db: None, redis_db: None) -> None:
    spans_storage = get_storage(StorageKey("eap_items"))
    messages = [
        gen_item_message(
            start_timestamp=BASE_TIME - timedelta(minutes=i),
            attributes={
                "str_tag": AnyValue(string_value=f"num: {i}"),
                "double_tag": AnyValue(double_value=1234567890.123),
                "sentry.segment_id": AnyValue(string_value=uuid.uuid4().hex[:16]),
            },
        )
        for i in range(120)
    ]

    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemDetails(BaseApiTest):
    def test_not_found(self, setup_logs_in_db: Any) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TraceItemDetailsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=0),
                end_timestamp=ts,
                request_id=_REQUEST_ID,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
            item_id="00000",
            trace_id=uuid.uuid4().hex,
        )
        response = self.app.post(
            "/rpc/EndpointTraceItemDetails/v1", data=message.SerializeToString()
        )
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 404, error_proto

    def test_missing_item_id(self, setup_logs_in_db: Any) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TraceItemDetailsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=0),
                end_timestamp=ts,
                request_id=_REQUEST_ID,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
            trace_id=uuid.uuid4().hex,
        )
        response = self.app.post(
            "/rpc/EndpointTraceItemDetails/v1", data=message.SerializeToString()
        )
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 400, error_proto

    def test_missing_trace_id(self, setup_logs_in_db: Any) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TraceItemDetailsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=0),
                end_timestamp=ts,
                request_id=_REQUEST_ID,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
            item_id="00000",
        )
        response = self.app.post(
            "/rpc/EndpointTraceItemDetails/v1", data=message.SerializeToString()
        )
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 400, error_proto

    def test_invalid_trace_id(self, setup_logs_in_db: Any) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TraceItemDetailsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=0),
                end_timestamp=ts,
                request_id=_REQUEST_ID,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
            item_id="00000",
            trace_id="baduuid",
        )
        response = self.app.post(
            "/rpc/EndpointTraceItemDetails/v1", data=message.SerializeToString()
        )
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 400, error_proto

    def test_endpoint_on_logs(self, setup_logs_in_db: Any) -> None:
        logs = (
            EndpointTraceItemTable()
            .execute(
                TraceItemTableRequest(
                    meta=RequestMeta(
                        project_ids=[1],
                        organization_id=1,
                        cogs_category="something",
                        referrer="something",
                        start_timestamp=START_TIMESTAMP,
                        end_timestamp=END_TIMESTAMP,
                        request_id=_REQUEST_ID,
                        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                    ),
                    columns=[
                        Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")
                        ),
                        Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.trace_id")
                        ),
                    ],
                )
            )
            .column_values
        )
        log_id = logs[0].results[0].val_str
        trace_id = logs[1].results[0].val_str

        res = EndpointTraceItemDetails().execute(
            TraceItemDetailsRequest(
                meta=RequestMeta(
                    project_ids=[1],
                    organization_id=1,
                    cogs_category="something",
                    referrer="something",
                    start_timestamp=START_TIMESTAMP,
                    end_timestamp=END_TIMESTAMP,
                    request_id=_REQUEST_ID,
                    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                ),
                item_id=log_id,
                trace_id=trace_id,
            )
        )
        attributes_returned = {x.name for x in res.attributes}

        for k in {
            "sentry.body",
            "sentry.severity_text",
            "sentry.severity_number",
            "sentry.organization_id",
            "sentry.project_id",
            "sentry.trace_id",
            "sentry.item_type",
            "sentry.timestamp_precise",
            "bool_tag",
            "double_tag",
            "int_tag",
            "str_tag",
        }:
            assert k in attributes_returned, k

    def test_endpoint_on_spans(self, setup_spans_in_db: Any) -> None:
        end = Timestamp()
        start = Timestamp()
        start.FromDatetime(BASE_TIME)
        end.GetCurrentTime()

        spans = (
            EndpointTraceItemTable()
            .execute(
                TraceItemTableRequest(
                    meta=RequestMeta(
                        project_ids=[1],
                        organization_id=1,
                        cogs_category="something",
                        referrer="something",
                        start_timestamp=start,
                        end_timestamp=end,
                        request_id=_REQUEST_ID,
                        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    ),
                    columns=[
                        Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")
                        ),
                        Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.trace_id")
                        ),
                    ],
                )
            )
            .column_values
        )
        span_id = spans[0].results[0].val_str
        trace_id = spans[1].results[0].val_str

        res = EndpointTraceItemDetails().execute(
            TraceItemDetailsRequest(
                meta=RequestMeta(
                    project_ids=[1],
                    organization_id=1,
                    cogs_category="something",
                    referrer="something",
                    start_timestamp=start,
                    end_timestamp=end,
                    request_id=_REQUEST_ID,
                    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                ),
                item_id=span_id,
                trace_id=trace_id,
            )
        )
        attributes_returned = {x.name for x in res.attributes}

        for k in {
            "double_tag",
            "sentry.duration_ms",
            "sentry.end_timestamp_precise",
            "sentry.event_id",
            "sentry.exclusive_time_ms",
            "sentry.is_segment",
            "sentry.item_type",
            "sentry.organization_id",
            "sentry.project_id",
            "sentry.raw_description",
            "sentry.received",
            "sentry.segment_id",
            "sentry.start_timestamp_precise",
            "sentry.trace_id",
            "str_tag",
        }:
            assert k in attributes_returned, k


def test_convert_results_dedupes() -> None:
    """
    Makes sure that _convert_results dedupes int/bool and float
    attributes. We store float versions of int and bool attributes
    for computational reasons but we don't want to return the
    duplicate float attrs to the user.
    """
    data = [
        {
            "timestamp": 1750964400,
            "hex_item_id": "e70ef5b1b5bc4611840eff9964b7a767",
            "trace_id": "cb190d6e7d5743d5bc1494c650592cd2",
            "organization_id": 1,
            "project_id": 1,
            "item_type": 1,
            "attributes_string": {
                "relay_protocol_version": "3",
                "sentry.segment_id": "30c64b1f21b54799",
            },
            "attributes_int": {"sentry.duration_ms": 152},
            "attributes_float": {"sentry.is_segment": 1.0, "num_of_spans": 50.0},
            "attributes_bool": {
                "my.true.bool.field": True,
                "sentry.is_segment": True,
                "my.false.bool.field": False,
            },
        }
    ]
    _, _, attrs = _convert_results(data)
    is_segment_attrs = list(filter(lambda x: x.name == "sentry.is_segment", attrs))
    assert len(is_segment_attrs) == 1
