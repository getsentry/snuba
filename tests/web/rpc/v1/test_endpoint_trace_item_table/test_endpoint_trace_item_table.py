import random
import uuid
from datetime import datetime, timedelta
from typing import Any, Mapping

import pytest
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.timestamp_pb2 import Timestamp
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
    VirtualColumnContext,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    ExistsFilter,
    OrFilter,
    TraceItemFilter,
)

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.endpoint_trace_item_table import (
    EndpointTraceItemTable,
    _apply_labels_to_columns,
)
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events

_RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"
_SERVER_NAME = "D23CXQ4GK2.local"


def gen_message(
    dt: datetime,
    measurements: dict[str, dict[str, float]] | None = None,
    tags: dict[str, str] | None = None,
) -> Mapping[str, Any]:
    measurements = measurements or {}
    tags = tags or {}
    return {
        "description": "/api/0/relays/projectconfigs/",
        "duration_ms": 152,
        "event_id": "d826225de75d42d6b2f01b957d51f18f",
        "exclusive_time_ms": 0.228,
        "is_segment": True,
        "data": {
            "sentry.environment": "development",
            "sentry.release": _RELEASE_TAG,
            "thread.name": "uWSGIWorker1Core0",
            "thread.id": "8522009600",
            "sentry.segment.name": "/api/0/relays/projectconfigs/",
            "sentry.sdk.name": "sentry.python.django",
            "sentry.sdk.version": "2.7.0",
            "my.float.field": 101.2,
            "my.int.field": 2000,
            "my.neg.field": -100,
            "my.neg.float.field": -101.2,
            "my.true.bool.field": True,
            "my.false.bool.field": False,
        },
        "measurements": {
            "num_of_spans": {"value": 50.0},
            "eap.measurement": {"value": random.choice([1, 100, 1000])},
            **measurements,
        },
        "organization_id": 1,
        "origin": "auto.http.django",
        "project_id": 1,
        "received": 1721319572.877828,
        "retention_days": 90,
        "segment_id": "8873a98879faf06d",
        "sentry_tags": {
            "category": "http",
            "environment": "development",
            "op": "http.server",
            "platform": "python",
            "release": _RELEASE_TAG,
            "sdk.name": "sentry.python.django",
            "sdk.version": "2.7.0",
            "status": "ok",
            "status_code": "200",
            "thread.id": "8522009600",
            "thread.name": "uWSGIWorker1Core0",
            "trace.status": "ok",
            "transaction": "/api/0/relays/projectconfigs/",
            "transaction.method": "POST",
            "transaction.op": "http.server",
            "user": "ip:127.0.0.1",
        },
        "span_id": "123456781234567D",
        "tags": {
            "http.status_code": "200",
            "relay_endpoint_version": "3",
            "relay_id": "88888888-4444-4444-8444-cccccccccccc",
            "relay_no_cache": "False",
            "relay_protocol_version": "3",
            "relay_use_post_or_schedule": "True",
            "relay_use_post_or_schedule_rejected": "version",
            "server_name": _SERVER_NAME,
            "spans_over_limit": "False",
            "color": random.choice(["red", "green", "blue"]),
            "location": random.choice(["mobile", "frontend", "backend"]),
            **tags,
        },
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(dt.timestamp()) * 1000 - int(random.gauss(1000, 200)),
        "start_timestamp_precise": dt.timestamp(),
        "end_timestamp_precise": dt.timestamp() + 1,
    }


BASE_TIME = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
    minutes=180
)


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    spans_storage = get_storage(StorageKey("eap_spans"))
    start = BASE_TIME
    messages = [gen_message(start - timedelta(minutes=i)) for i in range(120)]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


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
                end_timestamp=ts,
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
                end_timestamp=ts,
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

    def test_with_data(self, setup_teardown: Any) -> None:
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
                    results=[AttributeValue(val_str=_SERVER_NAME) for _ in range(60)],
                )
            ],
            page_token=PageToken(offset=60),
            meta=ResponseMeta(request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480"),
        )
        assert response == expected_response

    def test_booleans_and_number_compares(self, setup_teardown: Any) -> None:
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
                        type=AttributeKey.TYPE_STRING, name="sentry.span_id"
                    )
                ),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="sentry.span_id"
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
                    results=[AttributeValue(val_bool=True) for _ in range(60)],
                ),
                TraceItemColumnValues(
                    attribute_name="sentry.span_id",
                    results=[
                        AttributeValue(val_str="123456781234567d") for _ in range(60)
                    ],
                ),
            ],
            page_token=PageToken(offset=60),
            meta=ResponseMeta(request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480"),
        )
        assert response == expected_response

    def test_with_virtual_columns(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        limit = 5
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
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
                    value_map={_RELEASE_TAG: "4.2.0.69"},
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
            meta=ResponseMeta(request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480"),
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

    def test_table_with_aggregates(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        # breakpoint()
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
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

        print("responseee", response.column_values)

        print(
            "RESULTSSS",
            [
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
            ],
        )

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

    def test_table_with_columns_not_in_groupby(self, setup_teardown: Any) -> None:
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

    def test_order_by_non_selected(self) -> None:
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

    def test_order_by_aggregation(self, setup_teardown: Any) -> None:
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

    def test_aggregation_on_attribute_column(self) -> None:
        spans_storage = get_storage(StorageKey("eap_spans"))
        start = BASE_TIME
        measurement_val = 420.0
        measurement = {"custom_measurement": {"value": measurement_val}}
        tags = {"custom_tag": "blah"}
        messages_w_measurement = [
            gen_message(
                start - timedelta(minutes=i), measurements=measurement, tags=tags
            )
            for i in range(120)
        ]
        messages_no_measurement = [
            gen_message(start - timedelta(minutes=i), tags=tags) for i in range(120)
        ]
        write_raw_unprocessed_events(spans_storage, messages_w_measurement + messages_no_measurement)  # type: ignore

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
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

    def test_different_column_label_and_attr_name(self, setup_teardown: Any) -> None:
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = Timestamp(seconds=int((BASE_TIME - timedelta(hours=1)).timestamp()))
        message = TraceItemTableRequest(
            meta=RequestMeta(
                organization_id=1,
                project_ids=[1],
                start_timestamp=hour_ago,
                end_timestamp=ts,
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

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = Timestamp(seconds=int((BASE_TIME - timedelta(hours=1)).timestamp()))
        err_req = {
            "meta": {
                "organizationId": "1",
                "referrer": "api.organization-events",
                "projectIds": ["1"],
                "startTimestamp": hour_ago.ToJsonString(),
                "endTimestamp": ts.ToJsonString(),
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

    def test_same_column_name(self) -> None:

        dt = BASE_TIME - timedelta(minutes=5)
        spans_storage = get_storage(StorageKey("eap_spans"))
        messages = [
            {
                "description": "foo",
                "duration_ms": 152,
                "event_id": "d826225de75d42d6b2f01b957d51f18f",
                "exclusive_time_ms": 0.228,
                "is_segment": True,
                "data": {},
                "measurements": {
                    "foo": {"value": 5},
                },
                "organization_id": 1,
                "origin": "auto.http.django",
                "project_id": 1,
                "received": 1721319572.877828,
                "retention_days": 90,
                "segment_id": "8873a98879faf06d",
                "sentry_tags": {
                    "status": "success",
                },
                "span_id": "123456781234567D",
                "tags": {"foo": "five", "rachelkey": "rachelval"},
                "trace_id": uuid.uuid4().hex,
                "start_timestamp_ms": int(dt.timestamp()) * 1000
                - int(random.gauss(1000, 200)),
                "start_timestamp_precise": dt.timestamp(),
                "end_timestamp_precise": dt.timestamp() + 1,
            }
        ]
        write_raw_unprocessed_events(spans_storage, messages)  # type: ignore

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = Timestamp(seconds=int((BASE_TIME - timedelta(hours=1)).timestamp()))
        err_req = {
            "meta": {
                "organizationId": "1",
                "referrer": "api.organization-events",
                "projectIds": ["1"],
                "startTimestamp": hour_ago.ToJsonString(),
                "endTimestamp": ts.ToJsonString(),
                "traceItemType": "TRACE_ITEM_TYPE_SPAN",
            },
            "columns": [
                {
                    "key": {"type": "TYPE_STRING", "name": "sentry.name"},
                    "label": "description",
                },
                {
                    "key": {"type": "TYPE_INT", "name": "foo"},
                    "label": "tags[foo,number]",
                },
                {
                    "key": {"type": "TYPE_STRING", "name": "foo"},
                    "label": "tags[foo,string]",
                },
                {"key": {"type": "TYPE_STRING", "name": "foo"}, "label": "tags[foo]"},
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
                {"type": "TYPE_INT", "name": "foo"},
                {"type": "TYPE_STRING", "name": "foo"},
                {"type": "TYPE_STRING", "name": "foo"},
                {"type": "TYPE_STRING", "name": "sentry.span_id"},
                {"type": "TYPE_STRING", "name": "project.name"},
            ],
            "virtualColumnContexts": [
                {
                    "fromColumnName": "sentry.project_id",
                    "toColumnName": "project.name",
                    "valueMap": {"4555075531898880": "bar"},
                }
            ],
        }

        err_msg = ParseDict(err_req, TraceItemTableRequest())
        result = EndpointTraceItemTable().execute(err_msg)

        assert result.column_values[1].attribute_name == "tags[foo,number]"
        assert result.column_values[1].results[0].val_int == 5

        assert result.column_values[2].attribute_name == "tags[foo,string]"
        assert result.column_values[2].results[0].val_str == "five"

        assert result.column_values[3].attribute_name == "tags[foo]"
        assert result.column_values[3].results[0].val_str == "five"

    def test_table_with_disallowed_group_by_columns(self, setup_teardown: Any) -> None:
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

    def test_aggregation_filter_basic(self, setup_teardown: Any) -> None:
        """
        This test ensures that aggregates are properly filtered out
        when using an aggregation filter `val > 350`.
        """
        # first I write new messages with different value of kylestags,
        # theres a different number of messages for each tag so that
        # each will have a different sum value when i do aggregate
        spans_storage = get_storage(StorageKey("eap_spans"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [gen_message(msg_timestamp, tags={"kylestag": "val1"}) for i in range(3)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val2"}) for i in range(12)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val3"}) for i in range(30)]
        )
        write_raw_unprocessed_events(spans_storage, messages)  # type: ignore

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
                    AttributeValue(val_float=1214.4),
                    AttributeValue(val_float=3036),
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
        spans_storage = get_storage(StorageKey("eap_spans"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [gen_message(msg_timestamp, tags={"kylestag": "val1"}) for i in range(3)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val2"}) for i in range(12)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val3"}) for i in range(30)]
        )
        write_raw_unprocessed_events(spans_storage, messages)  # type: ignore

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())

        base_message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
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
                    AttributeValue(val_float=1214.4),
                    AttributeValue(val_float=3036),
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
                    AttributeValue(val_float=303.6),
                    AttributeValue(val_float=1214.4),
                    AttributeValue(val_float=3036),
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
        spans_storage = get_storage(StorageKey("eap_spans"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [gen_message(msg_timestamp, tags={"kylestag": "val1"}) for i in range(3)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val2"}) for i in range(12)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val3"}) for i in range(30)]
        )
        write_raw_unprocessed_events(spans_storage, messages)  # type: ignore

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


class TestUtils:
    def test_apply_labels_to_columns(self) -> None:
        message = TraceItemTableRequest(
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
                Column(
                    aggregation=AttributeAggregation(
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
