import random
from datetime import datetime, timedelta
from math import inf
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from clickhouse_driver.errors import ServerException
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.downsampled_storage_pb2 import (
    DownsampledStorageConfig,
    DownsampledStorageMeta,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationAndFilter,
    AggregationComparisonFilter,
    AggregationFilter,
    AggregationOrFilter,
    Column,
    TraceItemColumnValues,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.formula_pb2 import Literal
from sentry_protos.snuba.v1.request_common_pb2 import (
    PageToken,
    RequestMeta,
    ResponseMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    AttributeValue,
    ExtrapolationMode,
    Function,
    Reliability,
    StrArray,
    VirtualColumnContext,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    ExistsFilter,
    NotFilter,
    OrFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web import QueryException
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.endpoint_trace_item_table import (
    EndpointTraceItemTable,
    _apply_labels_to_columns,
)
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import (
    BASE_TIME,
    END_TIMESTAMP,
    RELEASE_TAG,
    SERVER_NAME,
    START_TIMESTAMP,
    gen_item_message,
    write_eap_item,
)

_SPAN_COUNT = 120


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    items_storage = get_storage(StorageKey("eap_items"))
    messages = [
        gen_item_message(
            start_timestamp=BASE_TIME + timedelta(minutes=i),
            item_id=int("123456781234567d", 16).to_bytes(16, byteorder="little"),
            attributes={
                "color": AnyValue(
                    string_value=random.choice(
                        [
                            "red",
                            "green",
                            "blue",
                        ]
                    )
                ),
                "eap.measurement": AnyValue(
                    int_value=random.choice(
                        [
                            1,
                            100,
                            1000,
                        ]
                    )
                ),
                "location": AnyValue(
                    string_value=random.choice(
                        [
                            "mobile",
                            "frontend",
                            "backend",
                        ]
                    )
                ),
                "custom_measurement": AnyValue(double_value=420.0),
                "custom_tag": AnyValue(string_value="blah"),
            },
        )
        for i in range(_SPAN_COUNT)
    ]
    write_raw_unprocessed_events(items_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemTable(BaseApiTest):
    def test_basic(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                )
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location"))
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                    )
                )
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

    def test_OOM(self, monkeypatch: Any) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                )
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location"))
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                    )
                )
            ],
            limit=10,
        )
        metrics_mock = MagicMock()
        monkeypatch.setattr(RPCEndpoint, "metrics", property(lambda x: metrics_mock))
        with (
            patch(
                "clickhouse_driver.client.Client.execute",
                side_effect=ServerException(
                    "DB::Exception: Received from snuba-events-analytics-platform-1-1:1111. DB::Exception: Memory limit (for query) exceeded: would use 1.11GiB (attempt to allocate chunk of 111111 bytes), maximum: 1.11 GiB. Blahblahblahblahblahblahblah",
                    code=241,
                ),
            ),
            patch("snuba.web.rpc.sentry_sdk.capture_exception") as sentry_sdk_mock,
        ):
            with pytest.raises(QueryException) as e:
                EndpointTraceItemTable().execute(message)
            assert "DB::Exception: Memory limit (for query) exceeded" in str(e.value)

            sentry_sdk_mock.assert_called()
            assert metrics_mock.increment.call_args_list.count(call("OOM_query")) == 1

    def test_timeoutt(self, monkeypatch: Any) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_BEST_EFFORT
                ),
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                )
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location"))
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                    )
                )
            ],
            limit=10,
        )
        metrics_mock = MagicMock()
        monkeypatch.setattr(RPCEndpoint, "metrics", property(lambda x: metrics_mock))
        with (
            patch(
                "clickhouse_driver.client.Client.execute",
                side_effect=ServerException(
                    "DB::Exception: Timeout exceeded: elapsed 32.8457984 seconds, maximum: 30: Blahblahblahblahblahblahblah",
                    code=159,
                ),
            ),
            patch("snuba.web.rpc.sentry_sdk.capture_exception") as sentry_sdk_mock,
        ):
            with pytest.raises(QueryException) as e:
                EndpointTraceItemTable().execute(message)
            assert "DB::Exception: Timeout exceeded" in str(e.value)

            sentry_sdk_mock.assert_called()
            metrics_mock.increment.assert_any_call(
                "timeout_query",
                1,
                {
                    "endpoint": "EndpointTraceItemTable",
                    "storage_routing_mode": "MODE_BEST_EFFORT",
                },
            )

    def test_errors_without_type(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=ts,
                end_timestamp=END_TIMESTAMP,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                )
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location"))
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                    )
                )
            ],
            limit=10,
        )
        response = self.app.post(
            "/rpc/EndpointTraceItemTable/v1", data=message.SerializeToString()
        )
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 400, error_proto

    def test_invalid_orderby_label(self, setup_teardown: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="server_name")
                )
            ],
            order_by=[TraceItemTableRequest.OrderBy(column=Column(label="some_label"))],
        )
        with pytest.raises(BadSnubaRPCRequestException) as excinfo:
            EndpointTraceItemTable().execute(message)
        assert (
            str(excinfo.value)
            == "Ordered by columns {'some_label'} not selected: {'server_name'}"
        )

    def test_with_orderby_label(self, setup_teardown: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="server_name")
                )
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(column=Column(label="server_name"))
            ],
        )
        response = EndpointTraceItemTable().execute(message)
        expected_response = TraceItemTableResponse(
            column_values=[
                TraceItemColumnValues(
                    attribute_name="server_name",
                    results=[
                        AttributeValue(val_str=SERVER_NAME) for _ in range(_SPAN_COUNT)
                    ],
                )
            ],
            page_token=PageToken(offset=_SPAN_COUNT),
            meta=ResponseMeta(
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                downsampled_storage_meta=DownsampledStorageMeta(
                    can_go_to_higher_accuracy_tier=False,
                ),
            ),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_with_data(self, setup_teardown: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="server_name")
                )
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="server_name"
                        )
                    )
                )
            ],
        )
        response = EndpointTraceItemTable().execute(message)
        expected_response = TraceItemTableResponse(
            column_values=[
                TraceItemColumnValues(
                    attribute_name="server_name",
                    results=[
                        AttributeValue(val_str=SERVER_NAME) for _ in range(_SPAN_COUNT)
                    ],
                )
            ],
            page_token=PageToken(offset=_SPAN_COUNT),
            meta=ResponseMeta(
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                downsampled_storage_meta=DownsampledStorageMeta(
                    can_go_to_higher_accuracy_tier=False,
                ),
            ),
        )
        assert MessageToDict(response) == MessageToDict(expected_response)

    def test_booleans_and_number_compares(self, setup_teardown: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                or_filter=OrFilter(
                    filters=[
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="eap.measurement",
                                ),
                                op=ComparisonFilter.OP_LESS_THAN_OR_EQUALS,
                                value=AttributeValue(val_double=101),
                            ),
                        ),
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="eap.measurement",
                                ),
                                op=ComparisonFilter.OP_GREATER_THAN,
                                value=AttributeValue(val_double=999),
                            ),
                        ),
                    ]
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_BOOLEAN, name="sentry.is_segment"
                    )
                ),
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.item_id"
                    )
                ),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="sentry.item_id"
                        )
                    )
                )
            ],
            limit=61,
        )
        response = EndpointTraceItemTable().execute(message)
        expected_response = TraceItemTableResponse(
            column_values=[
                TraceItemColumnValues(
                    attribute_name="sentry.is_segment",
                    results=[AttributeValue(val_bool=True) for _ in range(61)],
                ),
                TraceItemColumnValues(
                    attribute_name="sentry.item_id",
                    results=[
                        AttributeValue(val_str="123456781234567d") for _ in range(61)
                    ],
                ),
            ],
            page_token=PageToken(offset=61),
            meta=ResponseMeta(
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                downsampled_storage_meta=DownsampledStorageMeta(
                    can_go_to_higher_accuracy_tier=False,
                ),
            ),
        )
        assert response == expected_response

    def test_with_virtual_columns(self, setup_teardown: Any) -> None:
        limit = 5
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.category"
                    )
                )
            ),
            columns=(
                [
                    Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="sentry.project_name"
                        )
                    ),
                    Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="sentry.release_version"
                        )
                    ),
                    Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="sentry.sdk.name"
                        )
                    ),
                ]
            ),
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="sentry.project_name"
                        )
                    ),
                )
            ],
            limit=limit,
            virtual_column_contexts=[
                VirtualColumnContext(
                    from_column_name="sentry.project_id",
                    to_column_name="sentry.project_name",
                    value_map={"1": "sentry", "2": "snuba"},
                ),
                VirtualColumnContext(
                    from_column_name="sentry.release",
                    to_column_name="sentry.release_version",
                    value_map={RELEASE_TAG: "4.2.0.69"},
                ),
            ],
        )
        response = EndpointTraceItemTable().execute(message)
        expected_response = TraceItemTableResponse(
            column_values=[
                TraceItemColumnValues(
                    attribute_name="sentry.project_name",
                    results=[AttributeValue(val_str="sentry") for _ in range(limit)],
                ),
                TraceItemColumnValues(
                    attribute_name="sentry.release_version",
                    results=[AttributeValue(val_str="4.2.0.69") for _ in range(limit)],
                ),
                TraceItemColumnValues(
                    attribute_name="sentry.sdk.name",
                    results=[
                        AttributeValue(val_str="sentry.python.django")
                        for _ in range(limit)
                    ],
                ),
            ],
            page_token=PageToken(offset=limit),
            meta=ResponseMeta(
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                downsampled_storage_meta=DownsampledStorageMeta(
                    can_go_to_higher_accuracy_tier=False,
                ),
            ),
        )
        assert response.page_token == expected_response.page_token
        # make sure columns are ordered in the order they are requested
        assert [c.attribute_name for c in response.column_values] == [
            "sentry.project_name",
            "sentry.release_version",
            "sentry.sdk.name",
        ]
        assert response == expected_response, (
            MessageToDict(response),
            MessageToDict(expected_response),
        )

    def test_order_by_virtual_columns(self, setup_teardown: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.category"
                    )
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="special_color"
                    )
                ),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="special_color"
                        )
                    )
                )
            ],
            limit=61,
            virtual_column_contexts=[
                VirtualColumnContext(
                    from_column_name="color",
                    to_column_name="special_color",
                    value_map={"red": "1", "green": "2", "blue": "3"},
                ),
            ],
        )
        response = EndpointTraceItemTable().execute(message)
        result_colors = [c.val_str for c in response.column_values[0].results]
        assert sorted(result_colors) == result_colors

    def test_table_with_aggregates_backward_compat(self, setup_teardown: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.category"
                    )
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_MAX,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="my.float.field"
                        ),
                        label="max(my.float.field)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="my.float.field"
                        ),
                        label="avg(my.float.field)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="location")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                    )
                ),
            ],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="location",
                results=[
                    AttributeValue(val_str="backend"),
                    AttributeValue(val_str="frontend"),
                    AttributeValue(val_str="mobile"),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="max(my.float.field)",
                results=[
                    AttributeValue(val_double=101.2),
                    AttributeValue(val_double=101.2),
                    AttributeValue(val_double=101.2),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="avg(my.float.field)",
                results=[
                    AttributeValue(val_double=101.2),
                    AttributeValue(val_double=101.2),
                    AttributeValue(val_double=101.2),
                ],
            ),
        ]

    def test_table_with_aggregates(self, setup_teardown: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.category"
                    )
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_MAX,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="my.float.field"
                        ),
                        label="max(my.float.field)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="my.float.field"
                        ),
                        label="avg(my.float.field)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="location")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                    )
                ),
            ],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)

        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="location",
                results=[
                    AttributeValue(val_str="backend"),
                    AttributeValue(val_str="frontend"),
                    AttributeValue(val_str="mobile"),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="max(my.float.field)",
                results=[
                    AttributeValue(val_double=101.2),
                    AttributeValue(val_double=101.2),
                    AttributeValue(val_double=101.2),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="avg(my.float.field)",
                results=[
                    AttributeValue(val_double=101.2),
                    AttributeValue(val_double=101.2),
                    AttributeValue(val_double=101.2),
                ],
            ),
        ]

    def test_table_with_columns_not_in_groupby_backward_compat(
        self, setup_teardown: Any
    ) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_MAX,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="my.float.field"
                        ),
                        label="max(my.float.field)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    )
                ),
            ],
            group_by=[],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                    )
                ),
            ],
            limit=5,
        )
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTraceItemTable().execute(message)

    def test_table_with_columns_not_in_groupby(self, setup_teardown: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_MAX,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="my.float.field"
                        ),
                        label="max(my.float.field)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    )
                ),
            ],
            group_by=[],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                    )
                ),
            ],
            limit=5,
        )
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTraceItemTable().execute(message)

    def test_order_by_non_selected_backward_compat(self) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.category"
                    )
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="eap.measurement"
                        ),
                        label="avg(eap.measurment)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    )
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="location")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        aggregation=AttributeAggregation(
                            aggregate=Function.FUNCTION_MAX,
                            key=AttributeKey(
                                type=AttributeKey.TYPE_FLOAT, name="my.float.field"
                            ),
                            label="max(my.float.field)",
                            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                        )
                    )
                ),
            ],
            limit=5,
        )
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTraceItemTable().execute(message)

    def test_order_by_non_selected(self) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.category"
                    )
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="eap.measurement"
                        ),
                        label="avg(eap.measurment)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    )
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="location")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        aggregation=AttributeAggregation(
                            aggregate=Function.FUNCTION_MAX,
                            key=AttributeKey(
                                type=AttributeKey.TYPE_DOUBLE, name="my.float.field"
                            ),
                            label="max(my.float.field)",
                            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                        )
                    )
                ),
            ],
            limit=5,
        )
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTraceItemTable().execute(message)

    def test_order_by_aggregation_backward_compat(self, setup_teardown: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.category"
                    )
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="eap.measurement"
                        ),
                        label="avg(eap.measurment)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    )
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="location")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        aggregation=AttributeAggregation(
                            aggregate=Function.FUNCTION_AVG,
                            key=AttributeKey(
                                type=AttributeKey.TYPE_FLOAT, name="eap.measurement"
                            ),
                            label="avg(eap.measurment)",
                            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                        )
                    )
                ),
            ],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        measurements = [v.val_double for v in response.column_values[1].results]
        assert sorted(measurements) == measurements

    def test_order_by_aggregation(self, setup_teardown: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.category"
                    )
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="eap.measurement"
                        ),
                        label="avg(eap.measurment)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    )
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="location")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        aggregation=AttributeAggregation(
                            aggregate=Function.FUNCTION_AVG,
                            key=AttributeKey(
                                type=AttributeKey.TYPE_DOUBLE, name="eap.measurement"
                            ),
                            label="avg(eap.measurment)",
                            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                        )
                    )
                ),
            ],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        measurements = [v.val_double for v in response.column_values[1].results]
        assert sorted(measurements) == measurements

    def test_order_by_does_not_error_if_groupby_exists(self) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.timestamp"
                    )
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="sentry.timestamp"
                        ),
                    )
                ),
            ],
            group_by=[
                AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.timestamp")
            ],
        )
        EndpointTraceItemTable().execute(message)

    def test_aggregation_on_attribute_column_backward_compat(
        self,
        setup_teardown: Any,
    ) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT,
                            name="custom_measurement",
                        ),
                        label="avg(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    )
                ),
            ],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        measurement_avg = [v.val_double for v in response.column_values[0].results][0]
        assert measurement_avg == 420

    def test_aggregation_on_attribute_column(
        self,
        setup_teardown: Any,
    ) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="avg(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    )
                ),
            ],
            order_by=[],
            limit=5,
        )
        response = EndpointTraceItemTable().execute(message)
        measurement_avg = [v.val_double for v in response.column_values[0].results][0]
        assert measurement_avg == 420

    def test_different_column_label_and_attr_name_backward_compat(
        self,
        setup_teardown: Any,
    ) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                organization_id=1,
                project_ids=[1],
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                referrer="something",
                cogs_category="something",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    key=AttributeKey(name="sentry.name", type=AttributeKey.TYPE_STRING),
                    label="description",
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="sentry.duration_ms"
                        ),
                    ),
                    label="count()",
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.name")],
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values[0].attribute_name == "description"
        assert response.column_values[1].attribute_name == "count()"

    def test_different_column_label_and_attr_name(
        self,
        setup_teardown: Any,
    ) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                organization_id=1,
                project_ids=[1],
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                referrer="something",
                cogs_category="something",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    key=AttributeKey(name="sentry.name", type=AttributeKey.TYPE_STRING),
                    label="description",
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="sentry.duration_ms"
                        ),
                    ),
                    label="count()",
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.name")],
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values[0].attribute_name == "description"
        assert response.column_values[1].attribute_name == "count()"

    def test_cast_bug(self, setup_teardown: Any) -> None:
        """
        This test was added because the following request was causing a bug. The test was added when the bug was fixed.

        Specifically the bug was related to the 2nd and 3rd columns of the request:
        "type": "TYPE_INT", "name": "attr_num[foo]
        "type": "TYPE_BOOLEAN", "name": "attr_num[foo]
        and how alias was added to CAST.
        """

        err_req = {
            "meta": {
                "organizationId": "1",
                "referrer": "api.organization-events",
                "projectIds": ["1"],
                "startTimestamp": START_TIMESTAMP.ToJsonString(),
                "endTimestamp": END_TIMESTAMP.ToJsonString(),
                "traceItemType": "TRACE_ITEM_TYPE_SPAN",
            },
            "columns": [
                {
                    "key": {"type": "TYPE_STRING", "name": "sentry.name"},
                    "label": "description",
                },
                {
                    "key": {"type": "TYPE_INT", "name": "attr_num[foo]"},
                    "label": "tags[foo,number]",
                },
                {
                    "key": {"type": "TYPE_BOOLEAN", "name": "attr_num[foo]"},
                    "label": "tags[foo,boolean]",
                },
                {
                    "key": {"type": "TYPE_STRING", "name": "attr_str[foo]"},
                    "label": "tags[foo,string]",
                },
                {
                    "key": {"type": "TYPE_STRING", "name": "attr_str[foo]"},
                    "label": "tags[foo]",
                },
                {
                    "key": {"type": "TYPE_STRING", "name": "sentry.span_id"},
                    "label": "id",
                },
                {
                    "key": {"type": "TYPE_STRING", "name": "project.name"},
                    "label": "project.name",
                },
                {
                    "aggregation": {
                        "aggregate": "FUNCTION_COUNT",
                        "key": {"type": "TYPE_STRING", "name": "sentry.name"},
                    },
                    "label": "count()",
                },
            ],
            "orderBy": [
                {
                    "column": {
                        "key": {"type": "TYPE_STRING", "name": "sentry.name"},
                        "label": "description",
                    }
                }
            ],
            "groupBy": [
                {"type": "TYPE_STRING", "name": "sentry.name"},
                {"type": "TYPE_INT", "name": "attr_num[foo]"},
                {"type": "TYPE_BOOLEAN", "name": "attr_num[foo]"},
                {"type": "TYPE_STRING", "name": "attr_str[foo]"},
                {"type": "TYPE_STRING", "name": "attr_str[foo]"},
                {"type": "TYPE_STRING", "name": "sentry.span_id"},
                {"type": "TYPE_STRING", "name": "project.name"},
            ],
            "virtualColumnContexts": [
                {
                    "fromColumnName": "sentry.project_id",
                    "toColumnName": "project.name",
                    "valueMap": {"4554989665714177": "bar"},
                }
            ],
        }
        err_msg = ParseDict(err_req, TraceItemTableRequest())
        # just ensuring it doesnt raise an exception
        EndpointTraceItemTable().execute(err_msg)

    def test_table_with_disallowed_group_by_columns(self, setup_teardown: Any) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="location")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="location")
                    )
                ),
            ],
            limit=5,
        )
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTraceItemTable().execute(message)

    def test_table_with_group_by_columns_without_aggregation(
        self, setup_teardown: Any
    ) -> None:
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="timestamp")
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="timestamp")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="timestamp"
                        )
                    )
                ),
            ],
            limit=5,
        )
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTraceItemTable().execute(message)

    def test_aggregation_filter_basic_backward_compat(self) -> None:
        """
        This test ensures that aggregates are properly filtered out
        when using an aggregation filter `val > 350`.
        """
        # first I write new messages with different value of kylestags,
        # theres a different number of messages for each tag so that
        # each will have a different sum value when i do aggregate
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME + timedelta(minutes=1)
        messages = (
            [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val1")},
                )
                for _ in range(3)
            ]
            + [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val2")},
                )
                for i in range(12)
            ]
            + [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val3")},
                )
                for i in range(30)
            ]
        )
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="my.float.field"
                        ),
                        label="sum(my.float.field)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                    )
                ),
            ],
            aggregation_filter=AggregationFilter(
                comparison_filter=AggregationComparisonFilter(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="my.float.field"
                        ),
                        label="this-doesnt-matter-and-can-be-left-out",
                    ),
                    op=AggregationComparisonFilter.OP_GREATER_THAN,
                    val=350,
                )
            ),
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="kylestag",
                results=[
                    AttributeValue(val_str="val2"),
                    AttributeValue(val_str="val3"),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="sum(my.float.field)",
                results=[
                    AttributeValue(val_double=1214.4),
                    AttributeValue(val_double=3036),
                ],
            ),
        ]

    def test_aggregation_filter_basic(self) -> None:
        """
        This test ensures that aggregates are properly filtered out
        when using an aggregation filter `val > 350`.
        """
        # first I write new messages with different value of kylestags,
        # theres a different number of messages for each tag so that
        # each will have a different sum value when i do aggregate
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [
                gen_item_message(
                    msg_timestamp,
                    attributes={
                        "kylestag": AnyValue(string_value="val1"),
                    },
                )
                for i in range(3)
            ]
            + [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val2")},
                )
                for i in range(12)
            ]
            + [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val3")},
                )
                for i in range(30)
            ]
        )
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="my.float.field"
                        ),
                        label="sum(my.float.field)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                    )
                ),
            ],
            aggregation_filter=AggregationFilter(
                comparison_filter=AggregationComparisonFilter(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="my.float.field"
                        ),
                        label="this-doesnt-matter-and-can-be-left-out",
                    ),
                    op=AggregationComparisonFilter.OP_GREATER_THAN,
                    val=350,
                )
            ),
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="kylestag",
                results=[
                    AttributeValue(val_str="val2"),
                    AttributeValue(val_str="val3"),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="sum(my.float.field)",
                results=[
                    AttributeValue(val_double=1214.4),
                    AttributeValue(val_double=3036),
                ],
            ),
        ]

    def test_conditional_aggregation_in_select(self, setup_teardown: Any) -> None:
        """
        This test sums only if the traceitem contains kylestag = val2
        """
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val1")},
                )
                for i in range(3)
            ]
            + [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val2")},
                )
                for i in range(4)
            ]
            + [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val3")},
                )
                for i in range(3)
            ]
        )
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                ),
                Column(
                    conditional_aggregation=AttributeConditionalAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="my.float.field"
                        ),
                        label="sum(my.float.field)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                        filter=TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING, name="kylestag"
                                ),
                                op=ComparisonFilter.OP_EQUALS,
                                value=AttributeValue(val_str="val2"),
                            )
                        ),
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                    )
                ),
            ],
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="kylestag",
                results=[
                    AttributeValue(val_str="val1"),
                    AttributeValue(val_str="val2"),
                    AttributeValue(val_str="val3"),
                    AttributeValue(is_null=True),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="sum(my.float.field)",
                results=[
                    AttributeValue(is_null=True),
                    AttributeValue(val_double=404.8),
                    AttributeValue(is_null=True),
                    AttributeValue(is_null=True),
                ],
            ),
        ]

    def test_reliability_with_conditional_aggregation(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = [
            gen_item_message(
                msg_timestamp,
                server_sample_rate=0.1,
            ),
            gen_item_message(
                msg_timestamp,
                server_sample_rate=0.85,
            ),
        ]
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="sentry.sampling_weight"
                        ),
                        label="avg_sample(sampling_weight)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                    label="avg_sample(sampling_weight)",
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="sentry.duration_ms"
                        ),
                        label="count()",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    ),
                    label="count()",
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_MIN,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="sentry.sampling_weight"
                        ),
                        label="min(sampling_weight)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    ),
                    label="min(sampling_weight)",
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="sentry.duration_ms"
                        ),
                        label="count_sample()",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                    ),
                    label="count_sample()",
                ),
            ],
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="avg_sample(sampling_weight)",
                results=[
                    AttributeValue(val_double=5.5),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="count()",
                results=[
                    AttributeValue(val_double=11),
                ],
                reliabilities=[Reliability.RELIABILITY_LOW],
            ),
            TraceItemColumnValues(
                attribute_name="min(sampling_weight)",
                results=[
                    AttributeValue(val_double=1),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="count_sample()",
                results=[
                    AttributeValue(val_double=11),
                ],
                reliabilities=[Reliability.RELIABILITY_LOW],
            ),
        ]

    def test_aggregation_filter_and_or_backward_compat(
        self, setup_teardown: Any
    ) -> None:
        """
        This test ensures that aggregates are properly filtered out
        when using an aggregation filter `val > 350 and val > 350`.

        It also tests `val > 350 or val < 350`.
        """
        # first I write new messages with different value of kylestags,
        # theres a different number of messages for each tag so that
        # each will have a different sum value when i do aggregate
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val1")},
                )
                for i in range(3)
            ]
            + [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val2")},
                )
                for i in range(12)
            ]
            + [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val3")},
                )
                for i in range(30)
            ]
        )
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        base_message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="my.float.field"
                        ),
                        label="sum(my.float.field)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                    )
                ),
            ],
            aggregation_filter=AggregationFilter(  # same filter on both sides of the and
                and_filter=AggregationAndFilter(
                    filters=[
                        AggregationFilter(
                            comparison_filter=AggregationComparisonFilter(
                                aggregation=AttributeAggregation(
                                    aggregate=Function.FUNCTION_SUM,
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_FLOAT,
                                        name="my.float.field",
                                    ),
                                ),
                                op=AggregationComparisonFilter.OP_GREATER_THAN,
                                val=350,
                            )
                        ),
                        AggregationFilter(
                            comparison_filter=AggregationComparisonFilter(
                                aggregation=AttributeAggregation(
                                    aggregate=Function.FUNCTION_SUM,
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_FLOAT,
                                        name="my.float.field",
                                    ),
                                ),
                                op=AggregationComparisonFilter.OP_GREATER_THAN,
                                val=350,
                            )
                        ),
                    ]
                )
            ),
        )
        response = EndpointTraceItemTable().execute(base_message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="kylestag",
                results=[
                    AttributeValue(val_str="val2"),
                    AttributeValue(val_str="val3"),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="sum(my.float.field)",
                results=[
                    AttributeValue(val_double=1214.4),
                    AttributeValue(val_double=3036),
                ],
            ),
        ]

        # now try with an or filter
        base_message.aggregation_filter.CopyFrom(
            AggregationFilter(
                or_filter=AggregationOrFilter(
                    filters=[
                        AggregationFilter(
                            comparison_filter=AggregationComparisonFilter(
                                aggregation=AttributeAggregation(
                                    aggregate=Function.FUNCTION_SUM,
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_FLOAT,
                                        name="my.float.field",
                                    ),
                                ),
                                op=AggregationComparisonFilter.OP_GREATER_THAN,
                                val=350,
                            )
                        ),
                        AggregationFilter(
                            comparison_filter=AggregationComparisonFilter(
                                aggregation=AttributeAggregation(
                                    aggregate=Function.FUNCTION_SUM,
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_FLOAT,
                                        name="my.float.field",
                                    ),
                                ),
                                op=AggregationComparisonFilter.OP_LESS_THAN,
                                val=350,
                            )
                        ),
                    ]
                )
            )
        )
        response = EndpointTraceItemTable().execute(base_message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="kylestag",
                results=[
                    AttributeValue(val_str="val1"),
                    AttributeValue(val_str="val2"),
                    AttributeValue(val_str="val3"),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="sum(my.float.field)",
                results=[
                    AttributeValue(val_double=303.6),
                    AttributeValue(val_double=1214.4),
                    AttributeValue(val_double=3036),
                ],
            ),
        ]

    def test_aggregation_filter_and_or(self, setup_teardown: Any) -> None:
        """
        This test ensures that aggregates are properly filtered out
        when using an aggregation filter `val > 350 and val > 350`.

        It also tests `val > 350 or val < 350`.
        """
        # first I write new messages with different value of kylestags,
        # theres a different number of messages for each tag so that
        # each will have a different sum value when i do aggregate
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val1")},
                )
                for i in range(3)
            ]
            + [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val2")},
                )
                for i in range(12)
            ]
            + [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val3")},
                )
                for i in range(30)
            ]
        )
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        base_message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="my.float.field"
                        ),
                        label="sum(my.float.field)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                    )
                ),
            ],
            aggregation_filter=AggregationFilter(  # same filter on both sides of the and
                and_filter=AggregationAndFilter(
                    filters=[
                        AggregationFilter(
                            comparison_filter=AggregationComparisonFilter(
                                aggregation=AttributeAggregation(
                                    aggregate=Function.FUNCTION_SUM,
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_DOUBLE,
                                        name="my.float.field",
                                    ),
                                ),
                                op=AggregationComparisonFilter.OP_GREATER_THAN,
                                val=350,
                            )
                        ),
                        AggregationFilter(
                            comparison_filter=AggregationComparisonFilter(
                                aggregation=AttributeAggregation(
                                    aggregate=Function.FUNCTION_SUM,
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_DOUBLE,
                                        name="my.float.field",
                                    ),
                                ),
                                op=AggregationComparisonFilter.OP_GREATER_THAN,
                                val=350,
                            )
                        ),
                    ]
                )
            ),
        )
        response = EndpointTraceItemTable().execute(base_message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="kylestag",
                results=[
                    AttributeValue(val_str="val2"),
                    AttributeValue(val_str="val3"),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="sum(my.float.field)",
                results=[
                    AttributeValue(val_double=1214.4),
                    AttributeValue(val_double=3036),
                ],
            ),
        ]

        # now try with an or filter
        base_message.aggregation_filter.CopyFrom(
            AggregationFilter(
                or_filter=AggregationOrFilter(
                    filters=[
                        AggregationFilter(
                            comparison_filter=AggregationComparisonFilter(
                                aggregation=AttributeAggregation(
                                    aggregate=Function.FUNCTION_SUM,
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_DOUBLE,
                                        name="my.float.field",
                                    ),
                                ),
                                op=AggregationComparisonFilter.OP_GREATER_THAN,
                                val=350,
                            )
                        ),
                        AggregationFilter(
                            comparison_filter=AggregationComparisonFilter(
                                aggregation=AttributeAggregation(
                                    aggregate=Function.FUNCTION_SUM,
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_DOUBLE,
                                        name="my.float.field",
                                    ),
                                ),
                                op=AggregationComparisonFilter.OP_LESS_THAN,
                                val=350,
                            )
                        ),
                    ]
                )
            )
        )
        response = EndpointTraceItemTable().execute(base_message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="kylestag",
                results=[
                    AttributeValue(val_str="val1"),
                    AttributeValue(val_str="val2"),
                    AttributeValue(val_str="val3"),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="sum(my.float.field)",
                results=[
                    AttributeValue(val_double=303.6),
                    AttributeValue(val_double=1214.4),
                    AttributeValue(val_double=3036),
                ],
            ),
        ]

    def test_bad_aggregation_filter(self, setup_teardown: Any) -> None:
        """
        This test ensures that an error is raised when the aggregation filter is invalid.
        It tests a case where AttributeAggregation is set improperly.
        And it tests a case where an and filter has only one filter.
        """
        # first I write new messages with different value of kylestags,
        # theres a different number of messages for each tag so that
        # each will have a different sum value when i do aggregate
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val1")},
                )
                for i in range(3)
            ]
            + [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val2")},
                )
                for i in range(12)
            ]
            + [
                gen_item_message(
                    msg_timestamp,
                    attributes={"kylestag": AnyValue(string_value="val3")},
                )
                for i in range(30)
            ]
        )
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="my.float.field"
                        ),
                        label="sum(my.float.field)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kylestag")
                    )
                ),
            ],
            aggregation_filter=AggregationFilter(
                comparison_filter=AggregationComparisonFilter(
                    aggregation=AttributeAggregation(
                        label="sum(my.float.field)",
                    ),
                    op=AggregationComparisonFilter.OP_GREATER_THAN,
                    val=350,
                )
            ),
        )
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTraceItemTable().execute(message)

        # now do an and filter has only one filter
        message.aggregation_filter.CopyFrom(
            AggregationFilter(
                and_filter=AggregationAndFilter(
                    filters=[
                        AggregationFilter(
                            comparison_filter=AggregationComparisonFilter(
                                aggregation=AttributeAggregation(
                                    aggregate=Function.FUNCTION_SUM,
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_FLOAT,
                                        name="my.float.field",
                                    ),
                                ),
                                op=AggregationComparisonFilter.OP_LESS_THAN,
                                val=350,
                            )
                        ),
                    ]
                )
            )
        )
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTraceItemTable().execute(message)

    def test_offset_pagination(self, setup_teardown: Any) -> None:
        def make_request(page_token: PageToken) -> TraceItemTableRequest:
            return TraceItemTableRequest(
                meta=RequestMeta(
                    project_ids=[1, 2, 3],
                    organization_id=1,
                    cogs_category="something",
                    referrer="something",
                    start_timestamp=START_TIMESTAMP,
                    end_timestamp=END_TIMESTAMP,
                    request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                ),
                columns=[
                    Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="sentry.timestamp"
                        )
                    )
                ],
                order_by=[
                    TraceItemTableRequest.OrderBy(
                        column=Column(
                            key=AttributeKey(
                                type=AttributeKey.TYPE_STRING, name="sentry.timestamp"
                            )
                        )
                    )
                ],
                limit=5,
                page_token=page_token,
            )

        last_timestamp: datetime | None = None

        response = EndpointTraceItemTable().execute(make_request(PageToken(offset=0)))

        assert response.page_token == PageToken(offset=5)
        assert len(response.column_values) == 1
        assert response.column_values[0].attribute_name == "sentry.timestamp"

        # check that the timestamps in the results are in strictly increasing order
        for val in response.column_values[0].results:
            time = datetime.fromisoformat(val.val_str)
            if last_timestamp:
                assert time > last_timestamp
            else:
                last_timestamp = time

        assert last_timestamp is not None

        response = EndpointTraceItemTable().execute(make_request(PageToken(offset=5)))

        assert response.page_token == PageToken(offset=10)
        assert len(response.column_values) == 1
        assert response.column_values[0].attribute_name == "sentry.timestamp"

        # check that the timestamps in the results are in strictly increasing order
        # relative to the results from the first page
        for val in response.column_values[0].results:
            time = datetime.fromisoformat(val.val_str)
            assert time > last_timestamp

    def test_sparse_aggregate(self) -> None:
        """
        This test ensures that when aggregates are done, groups that dont have the attribute being
        aggregated over are not included in the response.

        ex:
        this should not return
        animal_type   |   sum(wing.count)
        bird          |   64
        chicken       |   12
        dog           |   0
        cat           |   0

        it should instead return
        animal_type   |   sum(wing.count)
        bird          |   64
        chicken       |   12

        because the dog and cat columns dont have attribute "wing.count"
        """
        span_ts = BASE_TIME - timedelta(minutes=1)
        write_eap_item(span_ts, {"animal_type": "bird", "wing.count": 2}, 10)
        write_eap_item(span_ts, {"animal_type": "chicken", "wing.count": 2}, 5)
        write_eap_item(span_ts, {"animal_type": "cat"}, 12)
        write_eap_item(span_ts, {"animal_type": "dog", "bark.db": 100}, 2)

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="animal_type")
                )
            ),
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="animal_type")
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="wing.count"
                        ),
                        label="sum(wing.count)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="bark.db"),
                        label="sum(bark.db)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="animal_type")],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="animal_type"
                        )
                    )
                ),
            ],
            limit=50,
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="animal_type",
                results=[
                    AttributeValue(val_str="bird"),
                    AttributeValue(val_str="chicken"),
                    AttributeValue(val_str="dog"),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="sum(wing.count)",
                results=[
                    AttributeValue(val_double=20),
                    AttributeValue(val_double=10),
                    AttributeValue(is_null=True),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="sum(bark.db)",
                results=[
                    AttributeValue(is_null=True),
                    AttributeValue(is_null=True),
                    AttributeValue(val_double=200),
                ],
            ),
        ]

    def test_agg_formula(self, setup_teardown: Any) -> None:
        """
        ensures formulas of aggregates work
        ex sum(my_attribute) / count(my_attribute)
        """
        span_ts = BASE_TIME - timedelta(minutes=1)
        write_eap_item(span_ts, {"kyles_measurement": 6}, 10)
        write_eap_item(span_ts, {"kyles_measurement": 7}, 2)

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_DOUBLE, name="kyles_measurement"
                    )
                )
            ),
            columns=[
                Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=Column(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="kyles_measurement",
                                ),
                            ),
                            label="sum(kyles_measurement)",
                        ),
                        right=Column(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_COUNT,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="kyles_measurement",
                                ),
                            ),
                            label="count(kyles_measurement)",
                        ),
                    ),
                    label="sum(kyles_measurement) / count(kyles_measurement)",
                ),
            ],
            limit=1,
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="sum(kyles_measurement) / count(kyles_measurement)",
                results=[
                    AttributeValue(val_double=(74 / 12)),
                ],
            ),
        ]

    def test_non_agg_formula(self, setup_teardown: Any) -> None:
        """
        ensures formulas of non-aggregates work
        ex: my_attribute + my_other_attribute
        """
        span_ts = BASE_TIME - timedelta(minutes=1)
        write_eap_item(span_ts, {"kyles_measurement": -1, "my_other_attribute": 1}, 4)
        write_eap_item(span_ts, {"kyles_measurement": 3, "my_other_attribute": 2}, 2)
        write_eap_item(span_ts, {"kyles_measurement": 10, "my_other_attribute": 3}, 1)

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_DOUBLE, name="kyles_measurement"
                    )
                )
            ),
            columns=[
                Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_ADD,
                        left=Column(
                            key=AttributeKey(
                                type=AttributeKey.TYPE_DOUBLE, name="kyles_measurement"
                            )
                        ),
                        right=Column(
                            key=AttributeKey(
                                type=AttributeKey.TYPE_DOUBLE, name="my_other_attribute"
                            )
                        ),
                    ),
                    label="kyles_measurement + my_other_attribute",
                ),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        formula=Column.BinaryFormula(
                            op=Column.BinaryFormula.OP_ADD,
                            left=Column(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="kyles_measurement",
                                )
                            ),
                            right=Column(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="my_other_attribute",
                                )
                            ),
                        ),
                        label="kyles_measurement + my_other_attribute",
                    )
                ),
            ],
            limit=50,
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="kyles_measurement + my_other_attribute",
                results=[
                    AttributeValue(val_double=0),
                    AttributeValue(val_double=0),
                    AttributeValue(val_double=0),
                    AttributeValue(val_double=0),
                    AttributeValue(val_double=5),
                    AttributeValue(val_double=5),
                    AttributeValue(val_double=13),
                ],
            ),
        ]

    def test_not_filter(setup_teardown: Any) -> None:
        span_ts = BASE_TIME - timedelta(minutes=1)
        write_eap_item(span_ts, {"attr1": "value1"}, 10)
        write_eap_item(span_ts, {"attr1": "value1", "attr2": "value2"}, 10)
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="attr1")),
            ],
            filter=TraceItemFilter(
                not_filter=NotFilter(
                    filters=[
                        TraceItemFilter(
                            exists_filter=ExistsFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING, name="attr2"
                                )
                            )
                        )
                    ]
                )
            ),
            limit=50,
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="attr1",
                results=[AttributeValue(val_str="value1") for _ in range(10)],
            ),
        ]

    def test_nonexistent_attribute(setup_teardown: Any) -> None:
        span_ts = BASE_TIME - timedelta(minutes=1)
        write_eap_item(span_ts, {"animal_type": "duck"}, 10)
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="nonexistent_string"
                    )
                ),
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_INT, name="nonexistent_int")
                ),
            ],
            limit=50,
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="nonexistent_string",
                results=[AttributeValue(is_null=True) for _ in range(10)],
            ),
            TraceItemColumnValues(
                attribute_name="nonexistent_int",
                results=[AttributeValue(is_null=True) for _ in range(10)],
            ),
        ]

    def test_calculate_http_response_rate(self) -> None:
        for i in range(9):
            span_ts = BASE_TIME - timedelta(seconds=i + 1)
            write_eap_item(span_ts, {"http.status_code": "200"}, 10)
        for i in range(10):
            span_ts = BASE_TIME - timedelta(seconds=i + 1)
            write_eap_item(span_ts, {"http.status_code": "502"}, 1)

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    label="http_response_rate_5",
                    formula=Column.BinaryFormula(
                        left=Column(
                            conditional_aggregation=AttributeConditionalAggregation(
                                aggregate=Function.FUNCTION_COUNT,
                                key=AttributeKey(
                                    name="http.status_code",
                                    type=AttributeKey.TYPE_STRING,
                                ),
                                filter=TraceItemFilter(
                                    comparison_filter=ComparisonFilter(
                                        key=AttributeKey(
                                            name="http.status_code",
                                            type=AttributeKey.TYPE_STRING,
                                        ),
                                        op=ComparisonFilter.OP_IN,
                                        value=AttributeValue(
                                            val_str_array=StrArray(
                                                values=[
                                                    "500",
                                                    "502",
                                                    "504",
                                                ],
                                            ),
                                        ),
                                    )
                                ),
                                label="error_request_count",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                            ),
                        ),
                        op=Column.BinaryFormula.OP_DIVIDE,
                        right=Column(
                            conditional_aggregation=AttributeConditionalAggregation(
                                aggregate=Function.FUNCTION_COUNT,
                                key=AttributeKey(
                                    name="http.status_code",
                                    type=AttributeKey.TYPE_STRING,
                                ),
                                label="total_request_count",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                            ),
                        ),
                    ),
                ),
            ],
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="http_response_rate_5",
                results=[AttributeValue(val_double=0.1)],
            ),
        ]

    def test_formula_aggregation(self) -> None:
        write_eap_item(BASE_TIME, {"kyles_tag": "a", "const_1": 1}, 10)
        write_eap_item(BASE_TIME, {"kyles_tag": "b", "const_1": 1}, 5)
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    label="count / avg",
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=Column(
                            conditional_aggregation=AttributeConditionalAggregation(
                                aggregate=Function.FUNCTION_COUNT,
                                key=AttributeKey(
                                    name="const_1",
                                    type=AttributeKey.TYPE_INT,
                                ),
                                label="count",
                            ),
                        ),
                        right=Column(
                            conditional_aggregation=AttributeConditionalAggregation(
                                aggregate=Function.FUNCTION_AVERAGE,
                                key=AttributeKey(
                                    name="const_1",
                                    type=AttributeKey.TYPE_INT,
                                ),
                                label="avg",
                            ),
                        ),
                    ),
                ),
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kyles_tag")
                ),
            ],
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="kyles_tag")
                )
            ),
            group_by=[
                AttributeKey(type=AttributeKey.TYPE_STRING, name="kyles_tag"),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="kyles_tag"
                        )
                    )
                )
            ],
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="count / avg",
                results=[
                    AttributeValue(val_double=10),
                    AttributeValue(val_double=5),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="kyles_tag",
                results=[AttributeValue(val_str="a"), AttributeValue(val_str="b")],
            ),
        ]

    def test_item_id_column(self) -> None:
        write_eap_item(
            BASE_TIME,
            item_id=int("123456781234567d", 16).to_bytes(16, byteorder="little"),
        )
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.item_id"
                    )
                ),
            ],
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.item_id"
                    ),
                    op=ComparisonFilter.OP_EQUALS,
                    value=AttributeValue(val_str="123456781234567d"),
                )
            ),
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="sentry.item_id",
                results=[AttributeValue(val_str="123456781234567d")],
            )
        ]

    def test_literal(self) -> None:
        span_ts = BASE_TIME - timedelta(minutes=1)
        write_eap_item(span_ts, {"measurement": 2}, 10)
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                exists_filter=ExistsFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="measurement")
                )
            ),
            columns=[
                Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=Column(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_DOUBLE,
                                    name="measurement",
                                ),
                            ),
                            label="sum(measurement)",
                        ),
                        right=Column(
                            literal=Literal(
                                val_double=5,
                            ),
                            label="5",
                        ),
                    ),
                    label="sum(measurement) / 5",
                ),
            ],
            limit=1,
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="sum(measurement) / 5",
                results=[
                    AttributeValue(val_double=(20 / 5)),
                ],
            ),
        ]

    def test_virtual_column_with_missing_attribute(self) -> None:
        span_ts = BASE_TIME - timedelta(minutes=1)
        write_eap_item(span_ts, {"attr1": "1"}, 10)
        write_eap_item(span_ts, {"attr2": "2"}, 5)
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="attr1_virtual"
                    )
                ),
            ],
            virtual_column_contexts=[
                VirtualColumnContext(
                    from_column_name="attr1",
                    to_column_name="attr1_virtual",
                    value_map={
                        "1": "a",
                        "2": "b",
                    },
                    default_value="default",
                ),
            ],
        )
        response = EndpointTraceItemTable().execute(message)
        assert sorted(response.column_values[0].results, key=lambda x: x.val_str) == [
            AttributeValue(val_str="a") for _ in range(10)
        ] + [AttributeValue(val_str="default") for _ in range(5)]

    def test_normal_mode_end_to_end(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = [
            gen_item_message(
                msg_timestamp,
                attributes={"endtoend": AnyValue(string_value="endtoend")},
            )
            for _ in range(3600)
        ]
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        best_effort_message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_NORMAL
                ),
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="endtoend"))
            ],
        )
        EndpointTraceItemTable().execute(best_effort_message)

    def test_downsampling_uses_hexintcolumnprocessor(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = [
            gen_item_message(
                msg_timestamp,
                attributes={"endtoend": AnyValue(string_value="endtoend")},
            )
            for _ in range(3600)
        ]
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        best_effort_message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_BEST_EFFORT
                ),
            ),
            columns=[
                Column(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.span_id"
                    ),
                    label="id",
                ),
            ],
        )

        # ensures we don't get DB::Exception: Illegal type UInt128 of argument of function right
        EndpointTraceItemTable().execute(best_effort_message)

    @pytest.mark.redis_db
    def test_non_existant_attribute_filter(self) -> None:
        """
        This test filters by env != "prod" and ensures that both "env"="dev" and "env"=None (attribute doesnt exist on the span) are returned.
        """
        span_ts = BASE_TIME - timedelta(minutes=1)
        write_eap_item(span_ts, {"env": "prod", "num_cats": 1})
        write_eap_item(span_ts, {"env": "dev", "num_cats": 2})
        write_eap_item(span_ts, {"num_cats": 3})

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="env"),
                    op=ComparisonFilter.OP_NOT_EQUALS,
                    value=AttributeValue(val_str="prod"),
                )
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="env")),
                Column(key=AttributeKey(type=AttributeKey.TYPE_INT, name="num_cats")),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="env")
                    )
                )
            ],
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="env",
                results=[
                    AttributeValue(val_str="dev"),
                    AttributeValue(is_null=True),
                ],
            ),
            TraceItemColumnValues(
                attribute_name="num_cats",
                results=[
                    AttributeValue(val_int=2),
                    AttributeValue(val_int=3),
                ],
            ),
        ]

    def test_formula_default(self) -> None:
        """
        ensures default values in formulas work
        """
        span_ts = BASE_TIME - timedelta(minutes=1)
        write_eap_item(span_ts, {"numerator": 10, "denominator": 2})
        write_eap_item(span_ts, {"numerator": 5})
        write_eap_item(span_ts, {"numerator": 1, "denominator": 0})

        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            columns=[
                Column(
                    formula=Column.BinaryFormula(
                        op=Column.BinaryFormula.OP_DIVIDE,
                        left=Column(
                            key=AttributeKey(
                                type=AttributeKey.TYPE_INT, name="numerator"
                            )
                        ),
                        right=Column(
                            key=AttributeKey(
                                type=AttributeKey.TYPE_INT, name="denominator"
                            )
                        ),
                        default_value_double=0.0,
                    ),
                    label="myformula",
                )
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        formula=Column.BinaryFormula(
                            op=Column.BinaryFormula.OP_DIVIDE,
                            left=Column(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_INT, name="numerator"
                                )
                            ),
                            right=Column(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_INT, name="denominator"
                                )
                            ),
                            default_value_double=0.0,
                        ),
                        label="myformula",
                    )
                )
            ],
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="myformula",
                results=[
                    AttributeValue(val_double=0.0),
                    AttributeValue(val_double=5),
                    AttributeValue(val_double=inf),
                ],
            ),
        ]


class TestUtils:
    def test_apply_labels_to_columns_backward_compat(self) -> None:
        message = TraceItemTableRequest(
            columns=[
                Column(
                    conditional_aggregation=AttributeConditionalAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="avg(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    )
                ),
                Column(
                    conditional_aggregation=AttributeConditionalAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
                        ),
                        label="avg(custom_measurement_2)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                    label=None,  # type: ignore
                ),
            ],
            order_by=[],
            limit=5,
        )
        _apply_labels_to_columns(message)
        assert message.columns[0].label == "avg(custom_measurement)"
        assert message.columns[1].label == "avg(custom_measurement_2)"

    def test_apply_labels_to_columns(self) -> None:
        message = TraceItemTableRequest(
            columns=[
                Column(
                    conditional_aggregation=AttributeConditionalAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="avg(custom_measurement)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    )
                ),
                Column(
                    conditional_aggregation=AttributeConditionalAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_DOUBLE, name="custom_measurement"
                        ),
                        label="avg(custom_measurement_2)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                    label=None,  # type: ignore
                ),
            ],
            order_by=[],
            limit=5,
        )
        _apply_labels_to_columns(message)
        assert message.columns[0].label == "avg(custom_measurement)"
        assert message.columns[1].label == "avg(custom_measurement_2)"
