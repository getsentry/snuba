import uuid
from datetime import datetime, timedelta
from typing import Any, Mapping, MutableMapping, Union

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageMeta
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemColumnValues,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    RequestMeta,
    ResponseMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events


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
        elif isinstance(v, float):
            attributes[k] = {
                "double_value": v,
            }

    return {
        "organization_id": 1,
        "project_id": 1,
        "timestamp_nanos": int(dt.timestamp() * 1e9),
        "observed_timestamp_nanos": int(dt.timestamp() * 1e9),
        "retention_days": 90,
        "body": body,
        "trace_id": str(uuid.uuid4()),
        "sampling_weight": 1,
        "span_id": "123456781234567D",
        "attributes": attributes,
    }


BASE_TIME = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
    minutes=180
)


@pytest.fixture(autouse=False)
def setup_logs_in_db(clickhouse_db: None, redis_db: None) -> None:
    logs_storage = get_storage(StorageKey("eap_items"))
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
class TestTraceItemTableForLogs(BaseApiTest):
    def test_with_logs_data(self, setup_logs_in_db: Any) -> None:
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_BOOLEAN, name="bool_tag"
                    ),  # this is true for every other log
                    value=AttributeValue(val_bool=True),
                    op=ComparisonFilter.OP_EQUALS,
                ),
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.body")
                ),
                Column(
                    key=AttributeKey(type=AttributeKey.Type.TYPE_INT, name="int_tag")
                ),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_INT, name="int_tag")
                    )
                )
            ],
            limit=20,
        )
        response = EndpointTraceItemTable().execute(message)

        expected_response = TraceItemTableResponse(
            column_values=[
                TraceItemColumnValues(
                    attribute_name="sentry.body",
                    results=[
                        AttributeValue(val_str=f"hello world {i}")
                        for i in range(2, 41, 2)
                    ],
                ),
                TraceItemColumnValues(
                    attribute_name="int_tag",
                    results=[AttributeValue(val_int=i) for i in range(2, 41, 2)],
                ),
            ],
            page_token=PageToken(offset=20),
            meta=ResponseMeta(
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                downsampled_storage_meta=DownsampledStorageMeta(
                    can_go_to_higher_accuracy_tier=False,
                ),
            ),
        )
        assert response == expected_response
