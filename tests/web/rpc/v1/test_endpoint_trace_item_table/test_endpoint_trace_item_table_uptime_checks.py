import uuid
from datetime import datetime, timedelta
from typing import Any, Mapping

import pytest
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemColumnValues,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    RequestMeta,
    ResponseMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events

_TRACE_ID = uuid.uuid4().hex


def gen_message(
    dt: datetime,
) -> Mapping[str, Any]:
    return {
        "organization_id": 1,
        "project_id": 1,
        "retention_days": 30,
        "region": "global",
        "environment": "prod",
        "subscription_id": uuid.uuid4().hex,
        "guid": uuid.uuid4().hex,
        "scheduled_check_time_ms": int(dt.timestamp() * 1e3),
        "actual_check_time_ms": int(dt.timestamp() * 1e3),
        "duration_ms": 1000,
        "status": "missed_window",
        "status_reason": {
            "type": "success",
            "description": "Some",
        },
        "http_status_code": 200,
        "trace_id": _TRACE_ID,
        "request_info": {
            "request_type": "GET",
            "http_status_code": 200,
        },
    }


BASE_TIME = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
    hours=1,
)


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    uptime_checks_storage = get_storage(StorageKey("uptime_monitor_checks"))
    messages = [gen_message(BASE_TIME - timedelta(minutes=i)) for i in range(200)]
    write_raw_unprocessed_events(uptime_checks_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemTable(BaseApiTest):
    def test_no_data(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_UPTIME_CHECK,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="region"))
            ],
            limit=10,
        )
        response = self.app.post(
            "/rpc/EndpointTraceItemTable/v1", data=message.SerializeToString()
        )
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 200, error_proto

    def test_with_data(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = Timestamp(
            seconds=int((BASE_TIME - timedelta(hours=10000)).timestamp())
        )
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=hour_ago,
                end_timestamp=ts,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_UPTIME_CHECK,
            ),
            columns=[
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING,
                        name="region",
                    ),
                ),
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING,
                        name="trace_id",
                    )
                ),
            ],
            limit=10,
        )
        response = EndpointTraceItemTable().execute(message)

        expected_response = TraceItemTableResponse(
            column_values=[
                TraceItemColumnValues(
                    attribute_name="region",
                    results=[AttributeValue(val_str="global") for _ in range(10)],
                ),
                TraceItemColumnValues(
                    attribute_name="trace_id",
                    results=[AttributeValue(val_str=_TRACE_ID) for _ in range(10)],
                ),
            ],
            page_token=PageToken(offset=10),
            meta=ResponseMeta(request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480"),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)
