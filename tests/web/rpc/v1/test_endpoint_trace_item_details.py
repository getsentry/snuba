import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping, MutableMapping, Union

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

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_details import EndpointTraceItemDetails
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events

_REQUEST_ID = uuid.uuid4().hex
_TRACE_ID = str(uuid.uuid4())


def gen_log_message(
    dt: datetime, tags: Mapping[str, Union[int, float, str, bool]], body: str
) -> MutableMapping[str, Any]:
    attributes: MutableMapping[str, Any] = {}
    for k, v in tags.items():
        if isinstance(v, bool):
            attributes[k] = {
                "bool_value": v,
            }
        elif isinstance(v, int):
            attributes[k] = {
                "int_value": v,
            }
        elif isinstance(v, float):
            attributes[k] = {"double_value": v}
        elif isinstance(v, str):
            attributes[k] = {
                "string_value": v,
            }

    return {
        "organization_id": 1,
        "project_id": 1,
        "timestamp_nanos": int(dt.timestamp() * 1e9),
        "observed_timestamp_nanos": int(dt.timestamp() * 1e9),
        "retention_days": 90,
        "body": body,
        "trace_id": _TRACE_ID,
        "sampling_weight": 1,
        "span_id": "123456781234567D",
        "attributes": attributes,
    }


def gen_span_message(
    dt: datetime,
    tags: Mapping[str, Union[int, float, str, bool]],
    numerical_attributes: dict[str, float],
) -> MutableMapping[str, Any]:
    return {
        "data": numerical_attributes,
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
        "tags": tags,
        "span_id": uuid.uuid4().hex,
        "trace_id": _TRACE_ID,
        "start_timestamp_ms": int(dt.timestamp()) * 1000,
        "start_timestamp_precise": dt.timestamp(),
        "end_timestamp_precise": dt.timestamp() + 1,
    }


BASE_TIME = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
    minutes=180
)


@pytest.fixture(autouse=False)
def setup_logs_in_db(clickhouse_db: None, redis_db: None) -> None:
    logs_storage = get_storage(StorageKey("eap_items_log"))
    messages = []
    for i in range(120):
        messages.append(
            gen_log_message(
                dt=BASE_TIME - timedelta(minutes=i),
                body=f"hello world {i}",
                tags={
                    "bool_tag": i % 2 == 0,
                    "int_tag": i,
                    "double_tag": float(i) / 2.0,
                    "str_tag": f"num: {i}",
                },
            )
        )
    write_raw_unprocessed_events(logs_storage, messages)  # type: ignore


@pytest.fixture(autouse=False)
def setup_spans_in_db(clickhouse_db: None, redis_db: None) -> None:
    spans_storage = get_storage(StorageKey("eap_items"))
    messages = []
    for i in range(120):
        messages.append(
            gen_span_message(
                dt=BASE_TIME - timedelta(minutes=i),
                tags={
                    "str_tag": f"num: {i}",
                },
                numerical_attributes={
                    "double_tag": float(i) / 2.0,
                },
            )
        )
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
        )
        response = self.app.post(
            "/rpc/EndpointTraceItemDetails/v1", data=message.SerializeToString()
        )
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 404, error_proto

    def test_endpoint_on_logs(self, setup_logs_in_db: Any) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()

        logs = (
            EndpointTraceItemTable()
            .execute(
                TraceItemTableRequest(
                    meta=RequestMeta(
                        project_ids=[1],
                        organization_id=1,
                        cogs_category="something",
                        referrer="something",
                        start_timestamp=Timestamp(seconds=0),
                        end_timestamp=ts,
                        request_id=_REQUEST_ID,
                        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                    ),
                    columns=[
                        Column(
                            key=AttributeKey(
                                type=AttributeKey.TYPE_STRING, name="sentry.item_id"
                            )
                        )
                    ],
                )
            )
            .column_values
        )
        log_id = logs[0].results[0].val_str

        res = EndpointTraceItemDetails().execute(
            TraceItemDetailsRequest(
                meta=RequestMeta(
                    project_ids=[1],
                    organization_id=1,
                    cogs_category="something",
                    referrer="something",
                    start_timestamp=Timestamp(seconds=0),
                    end_timestamp=ts,
                    request_id=_REQUEST_ID,
                    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                ),
                item_id=log_id,
            )
        )

        assert set(x.name for x in res.attributes) == {
            "sentry.body",
            "sentry.span_id",
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
        }

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
                            key=AttributeKey(
                                type=AttributeKey.TYPE_STRING, name="sentry.item_id"
                            )
                        )
                    ],
                )
            )
            .column_values
        )
        span_id = spans[0].results[0].val_str

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
            )
        )

        assert set(x.name for x in res.attributes) == {
            "sentry.trace_id",
            "sentry.organization_id",
            "sentry.project_id",
            "sentry.item_type",
            "sentry.segment_id",
            "sentry.raw_description",
            "sentry.event_id",
            "str_tag",
            "sentry.end_timestamp_precise",
            "sentry.duration_ms",
            "sentry.received",
            "sentry.exclusive_time_ms",
            "sentry.start_timestamp_precise",
            "sentry.is_segment",
            "double_tag",
            "sentry.duration_ms",
            "sentry.is_segment",
        }
