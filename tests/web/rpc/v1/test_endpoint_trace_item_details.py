import uuid
from datetime import datetime, timedelta
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


BASE_TIME = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
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

    def test_endpoint(self, setup_logs_in_db: Any) -> None:
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
            "bool_tag",
            "double_tag",
            "int_tag",
            "str_tag",
        }
