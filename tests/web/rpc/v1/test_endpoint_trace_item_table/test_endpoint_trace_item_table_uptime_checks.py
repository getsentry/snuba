import uuid
from datetime import datetime, timedelta
from operator import itemgetter
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
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageMeta


_TRACE_ID = uuid.uuid4().hex


def gen_message(
    dt: datetime,
    overrides: Mapping[str, Any] = {},
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
        "incident_status": 0,
        "request_info": {
            "request_type": "GET",
            "http_status_code": 200,
        },
        **overrides,
    }


BASE_TIME = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
    hours=1,
)
_UPTIME_CHECKS = [gen_message(BASE_TIME - timedelta(minutes=i)) for i in range(200)]


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    uptime_checks_storage = get_storage(StorageKey("uptime_monitor_checks"))
    write_raw_unprocessed_events(uptime_checks_storage, _UPTIME_CHECKS)  # type: ignore


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
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_INT,
                        name="scheduled_check_time",
                    )
                ),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_INT,
                            name="scheduled_check_time",
                        )
                    ),
                ),
            ],
            limit=10,
        )
        response = EndpointTraceItemTable().execute(message)
        checks = list(
            sorted(_UPTIME_CHECKS, key=itemgetter("scheduled_check_time_ms"))
        )[:10]

        expected_response = TraceItemTableResponse(
            column_values=[
                TraceItemColumnValues(
                    attribute_name="region",
                    results=[AttributeValue(val_str=c["region"]) for c in checks],
                ),
                TraceItemColumnValues(
                    attribute_name="trace_id",
                    results=[AttributeValue(val_str=c["trace_id"]) for c in checks],
                ),
                TraceItemColumnValues(
                    attribute_name="scheduled_check_time",
                    results=[
                        AttributeValue(val_int=int(c["scheduled_check_time_ms"] / 1e3))
                        for c in checks
                    ],
                ),
            ],
            page_token=PageToken(offset=10),
            meta=ResponseMeta(downsampled_storage_meta=DownsampledStorageMeta(can_go_to_higher_accuracy_tier=False), request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480"),
        )
        print("response", response)
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_offset(self, setup_teardown: Any) -> None:
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
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_INT,
                        name="scheduled_check_time",
                    )
                ),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_INT,
                            name="scheduled_check_time",
                        )
                    ),
                ),
            ],
            limit=5,
            page_token=PageToken(offset=5),  # Start from the 6th item
        )
        response = EndpointTraceItemTable().execute(message)
        checks = list(
            sorted(_UPTIME_CHECKS, key=itemgetter("scheduled_check_time_ms"))
        )[
            5:10
        ]  # Get items 6-10

        expected_response = TraceItemTableResponse(
            column_values=[
                TraceItemColumnValues(
                    attribute_name="region",
                    results=[AttributeValue(val_str=c["region"]) for c in checks],
                ),
                TraceItemColumnValues(
                    attribute_name="trace_id",
                    results=[AttributeValue(val_str=c["trace_id"]) for c in checks],
                ),
                TraceItemColumnValues(
                    attribute_name="scheduled_check_time",
                    results=[
                        AttributeValue(val_int=int(c["scheduled_check_time_ms"] / 1e3))
                        for c in checks
                    ],
                ),
            ],
            page_token=PageToken(offset=10),
            meta=ResponseMeta(downsampled_storage_meta=DownsampledStorageMeta(can_go_to_higher_accuracy_tier=False), request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480"),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_nonexistent_attribute() -> None:
    _UPTIME_CHECKS = [
        gen_message(
            BASE_TIME - timedelta(minutes=30),
            {
                "http_status_code": None,
                "request_info": {"request_type": "GET", "http_status_code": None},
            },
        )
        for i in range(50)
    ]
    write_raw_unprocessed_events(get_storage(StorageKey("uptime_monitor_checks")), _UPTIME_CHECKS)  # type: ignore
    ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
    hour_ago = Timestamp(seconds=int((BASE_TIME - timedelta(hours=1)).timestamp()))
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
                    type=AttributeKey.TYPE_INT,
                    name="http_status_code",
                ),
            ),
        ],
        limit=50,
    )
    response = EndpointTraceItemTable().execute(message)
    assert response.column_values == [
        TraceItemColumnValues(
            attribute_name="http_status_code",
            results=[AttributeValue(is_null=True) for _ in range(50)],
        ),
    ]
