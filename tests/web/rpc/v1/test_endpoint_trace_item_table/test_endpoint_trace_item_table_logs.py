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
    QueryInfo,
    QueryMetadata,
    QueryStats,
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

from snuba.datasets.storages.factory import get_writable_storage
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
def setup_logs_in_db(eap: None, redis_db: None) -> None:
    logs_storage = get_writable_storage(StorageKey("eap_items"))
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
    write_raw_unprocessed_events(logs_storage, messages)


@pytest.mark.eap
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
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.body")),
                Column(key=AttributeKey(type=AttributeKey.Type.TYPE_INT, name="int_tag")),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="int_tag"))
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
                    results=[AttributeValue(val_str=f"hello world {i}") for i in index_range],
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
                query_info=[
                    QueryInfo(
                        stats=QueryStats(
                            progress_bytes=response.meta.query_info[0].stats.progress_bytes
                        ),
                        metadata=QueryMetadata(),
                        trace_logs="",
                    )
                ],
            ),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)


@pytest.fixture(autouse=False)
def setup_bool_logs_in_db(eap: None, redis_db: None) -> None:
    """Three log groups: ``hasCodeTag`` false, true, and absent (getsentry/sentry#119735)."""
    logs_storage = get_writable_storage(StorageKey("eap_items"))
    messages = []
    for i in range(30):
        timestamp = BASE_TIME - timedelta(minutes=i)
        attributes = {"int_tag": AnyValue(int_value=i)}
        if i < 10:
            attributes["hasCodeTag"] = AnyValue(bool_value=False)
        elif i < 20:
            attributes["hasCodeTag"] = AnyValue(bool_value=True)
        messages.append(
            gen_item_message(
                start_timestamp=timestamp,
                remove_default_attributes=True,
                type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                attributes=attributes,
            )
        )
    write_raw_unprocessed_events(logs_storage, messages)


@pytest.mark.eap
@pytest.mark.redis_db
class TestBooleanAttributeFilteringForLogs(BaseApiTest):
    def _query_hascodetag(self, val: bool) -> TraceItemTableResponse:
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
                    key=AttributeKey(type=AttributeKey.TYPE_BOOLEAN, name="hasCodeTag"),
                    value=AttributeValue(val_bool=val),
                    op=ComparisonFilter.OP_EQUALS,
                ),
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.Type.TYPE_INT, name="int_tag")),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="int_tag"))
                )
            ],
            limit=50,
        )
        return EndpointTraceItemTable().execute(message)

    def test_equals_false_excludes_absent_attribute(self, setup_bool_logs_in_db: Any) -> None:
        response = self._query_hascodetag(False)
        (values,) = response.column_values
        returned = sorted(v.val_int for v in values.results)
        assert returned == list(range(10))

    def test_equals_true_returns_only_true(self, setup_bool_logs_in_db: Any) -> None:
        response = self._query_hascodetag(True)
        (values,) = response.column_values
        returned = sorted(v.val_int for v in values.results)
        assert returned == list(range(10, 20))

    def test_select_absent_attribute_is_null_not_false(self, setup_bool_logs_in_db: Any) -> None:
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
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.Type.TYPE_INT, name="int_tag"),
                    label="int_tag",
                ),
                Column(
                    key=AttributeKey(type=AttributeKey.Type.TYPE_BOOLEAN, name="hasCodeTag"),
                    label="hasCodeTag",
                ),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="int_tag"))
                )
            ],
            limit=50,
        )
        response = EndpointTraceItemTable().execute(message)
        by_label = {v.attribute_name: v for v in response.column_values}
        int_values = [v.val_int for v in by_label["int_tag"].results]
        by_int = dict(zip(int_values, by_label["hasCodeTag"].results, strict=True))

        for i in range(10):
            assert by_int[i].WhichOneof("value") == "val_bool" and by_int[i].val_bool is False
        for i in range(10, 20):
            assert by_int[i].WhichOneof("value") == "val_bool" and by_int[i].val_bool is True
        for i in range(20, 30):
            assert by_int[i].is_null is True and by_int[i].WhichOneof("value") != "val_bool"
