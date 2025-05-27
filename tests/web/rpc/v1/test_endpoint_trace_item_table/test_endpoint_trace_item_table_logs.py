from datetime import timedelta
from typing import Any

import pytest
from google.protobuf.json_format import MessageToDict
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
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import (
    BASE_TIME,
    END_TIMESTAMP,
    START_TIMESTAMP,
    gen_item_message,
)


@pytest.fixture(autouse=False)
def setup_logs_in_db(clickhouse_db: None, redis_db: None) -> None:
    logs_storage = get_storage(StorageKey("eap_items"))
    messages = []
    for i in range(120):
        timestamp = BASE_TIME - timedelta(minutes=i)
        messages.append(
            gen_item_message(
                start_timestamp=timestamp,
                remove_default_attributes=True,
                type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                attributes={
                    "bool_tag": AnyValue(bool_value=i % 2 == 0),
                    "double_tag": AnyValue(double_value=float(i) / 2.0),
                    "int_tag": AnyValue(int_value=i),
                    "str_tag": AnyValue(string_value=f"num: {i}"),
                    "sentry.body": AnyValue(string_value=f"hello world {i}"),
                    "sentry.severity_number": AnyValue(int_value=10),
                    "sentry.severity_text": AnyValue(string_value="info"),
                },
            )
        )
    write_raw_unprocessed_events(logs_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemTableForLogs(BaseApiTest):
    def test_with_logs_data(self, setup_logs_in_db: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
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
        index_range = range(0, 40, 2)
        expected_response = TraceItemTableResponse(
            column_values=[
                TraceItemColumnValues(
                    attribute_name="sentry.body",
                    results=[
                        AttributeValue(val_str=f"hello world {i}") for i in index_range
                    ],
                ),
                TraceItemColumnValues(
                    attribute_name="int_tag",
                    results=[AttributeValue(val_int=i) for i in index_range],
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
        assert MessageToDict(response) == MessageToDict(expected_response)
