import random
import uuid
from datetime import datetime, timedelta
from typing import Any, Mapping
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
from tests.conftest import SnubaSetConfig
from tests.helpers import write_raw_unprocessed_events

_RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"
_SERVER_NAME = "D23CXQ4GK2.local"


def gen_message(
    dt: datetime,
    measurements: dict[str, dict[str, float]] | None = None,
    tags: dict[str, str] | None = None,
    randomize_span_id: bool = False,
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
        "span_id": uuid.uuid4().hex if randomize_span_id else "123456781234567d",
        "tags": {
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


def write_eap_span(
    timestamp: datetime,
    attributes: dict[str, str | float] | None = None,
    count: int = 1,
) -> None:
    """
    This is a helper function to write a single or multiple eap-spans to the database.
    It uses gen_message to generate the spans and then writes them to the database.

    Args:
        timestamp: The timestamp of the span to write.
        attributes: attributes to go on the span.
        count: the number of these spans to write.
    """
    # convert attributes parameter into measurements and tags (what gen_message expects)
    measurements = None
    tags = None
    if attributes is not None:
        measurements = {}
        tags = {}
        for key, value in attributes.items():
            if isinstance(value, str):
                tags[key] = value
            else:
                measurements[key] = {"value": value}

    write_raw_unprocessed_events(
        get_storage(StorageKey("eap_spans")),  # type: ignore
        [
            gen_message(timestamp, measurements=measurements, tags=tags)
            for _ in range(count)
        ],
    )

    write_raw_unprocessed_events(
        get_storage(StorageKey("eap_items")),  # type: ignore
        [
            gen_message(timestamp, measurements=measurements, tags=tags)
            for _ in range(count)
        ],
    )


BASE_TIME = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
    minutes=180
)


@pytest.fixture(autouse=False)
def setup_teardown(clickhouse_db: None, redis_db: None) -> None:
    spans_storage = get_storage(StorageKey("eap_spans"))
    items_storage = get_storage(StorageKey("eap_items"))
    start = BASE_TIME
    messages = [gen_message(start - timedelta(minutes=i)) for i in range(120)]
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore
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
                end_timestamp=ts,
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
        breakpoint()
        assert response == expected_response

    def test_booleans_and_number_compares_backward_compat(
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
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=TraceItemFilter(
                or_filter=OrFilter(
                    filters=[
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_FLOAT,
                                    name="eap.measurement",
                                ),
                                op=ComparisonFilter.OP_LESS_THAN_OR_EQUALS,
                                value=AttributeValue(val_float=101),
                            ),
                        ),
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_FLOAT,
                                    name="eap.measurement",
                                ),
                                op=ComparisonFilter.OP_GREATER_THAN,
                                value=AttributeValue(val_float=999),
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

    def test_table_with_aggregates_backward_compat(self, setup_teardown: Any) -> None:
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

    def test_order_by_non_selected_backward_compat(self) -> None:
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

    def test_order_by_aggregation_backward_compat(self, setup_teardown: Any) -> None:
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

    def test_aggregation_on_attribute_column_backward_compat(self) -> None:
        spans_storage = get_storage(StorageKey("eap_spans"))
        items_storage = get_storage(StorageKey("eap_items"))
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
        write_raw_unprocessed_events(
            spans_storage,  # type: ignore
            messages_w_measurement + messages_no_measurement,
        )
        write_raw_unprocessed_events(
            items_storage,  # type: ignore
            messages_w_measurement + messages_no_measurement,
        )

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
                            type=AttributeKey.TYPE_FLOAT, name="custom_measurement"
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

    def test_aggregation_on_attribute_column(self) -> None:
        spans_storage = get_storage(StorageKey("eap_spans"))
        items_storage = get_storage(StorageKey("eap_items"))
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
        write_raw_unprocessed_events(
            spans_storage,  # type: ignore
            messages_w_measurement + messages_no_measurement,
        )
        write_raw_unprocessed_events(
            items_storage,  # type: ignore
            messages_w_measurement + messages_no_measurement,
        )

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

    def test_different_column_label_and_attr_name_backward_compat(
        self, setup_teardown: Any
    ) -> None:
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
        items_storage = get_storage(StorageKey("eap_items"))
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
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore
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

    def test_aggregation_filter_basic_backward_compat(
        self, setup_teardown: Any
    ) -> None:
        """
        This test ensures that aggregates are properly filtered out
        when using an aggregation filter `val > 350`.
        """
        # first I write new messages with different value of kylestags,
        # theres a different number of messages for each tag so that
        # each will have a different sum value when i do aggregate
        spans_storage = get_storage(StorageKey("eap_spans"))
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [gen_message(msg_timestamp, tags={"kylestag": "val1"}) for i in range(3)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val2"}) for i in range(12)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val3"}) for i in range(30)]
        )
        write_raw_unprocessed_events(spans_storage, messages)  # type: ignore
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

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
                    AttributeValue(val_double=1214.4),
                    AttributeValue(val_double=3036),
                ],
            ),
        ]

    def test_aggregation_filter_basic(self, setup_teardown: Any) -> None:
        """
        This test ensures that aggregates are properly filtered out
        when using an aggregation filter `val > 350`.
        """
        # first I write new messages with different value of kylestags,
        # theres a different number of messages for each tag so that
        # each will have a different sum value when i do aggregate
        spans_storage = get_storage(StorageKey("eap_spans"))
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [gen_message(msg_timestamp, tags={"kylestag": "val1"}) for i in range(3)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val2"}) for i in range(12)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val3"}) for i in range(30)]
        )
        write_raw_unprocessed_events(spans_storage, messages)  # type: ignore
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

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
        spans_storage = get_storage(StorageKey("eap_spans"))
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [gen_message(msg_timestamp, tags={"kylestag": "val1"}) for i in range(3)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val2"}) for i in range(4)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val3"}) for i in range(3)]
        )
        write_raw_unprocessed_events(spans_storage, messages)  # type: ignore
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

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
        spans_storage = get_storage(StorageKey("eap_spans"))
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = [
            gen_message(
                msg_timestamp, measurements={"client_sample_rate": {"value": 0.1}}
            ),
            gen_message(
                msg_timestamp, measurements={"client_sample_rate": {"value": 0.85}}
            ),
        ]
        write_raw_unprocessed_events(spans_storage, messages)  # type: ignore
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

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
        spans_storage = get_storage(StorageKey("eap_spans"))
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [gen_message(msg_timestamp, tags={"kylestag": "val1"}) for i in range(3)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val2"}) for i in range(12)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val3"}) for i in range(30)]
        )
        write_raw_unprocessed_events(spans_storage, messages)  # type: ignore
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

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
        spans_storage = get_storage(StorageKey("eap_spans"))
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [gen_message(msg_timestamp, tags={"kylestag": "val1"}) for i in range(3)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val2"}) for i in range(12)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val3"}) for i in range(30)]
        )
        write_raw_unprocessed_events(spans_storage, messages)  # type: ignore
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

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
        spans_storage = get_storage(StorageKey("eap_spans"))
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = (
            [gen_message(msg_timestamp, tags={"kylestag": "val1"}) for i in range(3)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val2"}) for i in range(12)]
            + [gen_message(msg_timestamp, tags={"kylestag": "val3"}) for i in range(30)]
        )
        write_raw_unprocessed_events(spans_storage, messages)  # type: ignore
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

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

    def test_offset_pagination(self, setup_teardown: Any) -> None:
        def make_request(page_token: PageToken) -> TraceItemTableRequest:
            ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
            hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
            return TraceItemTableRequest(
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

    def test_sparse_aggregate(self, setup_teardown: Any) -> None:
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
        write_eap_span(span_ts, {"animal_type": "bird", "wing.count": 2}, 10)
        write_eap_span(span_ts, {"animal_type": "chicken", "wing.count": 2}, 5)
        write_eap_span(span_ts, {"animal_type": "cat"}, 12)
        write_eap_span(span_ts, {"animal_type": "dog", "bark.db": 100}, 2)

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
        write_eap_span(span_ts, {"kyles_measurement": 6}, 10)
        write_eap_span(span_ts, {"kyles_measurement": 7}, 2)

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
        write_eap_span(span_ts, {"kyles_measurement": -1, "my_other_attribute": 1}, 4)
        write_eap_span(span_ts, {"kyles_measurement": 3, "my_other_attribute": 2}, 2)
        write_eap_span(span_ts, {"kyles_measurement": 10, "my_other_attribute": 3}, 1)

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
        write_eap_span(span_ts, {"attr1": "value1"}, 10)
        write_eap_span(span_ts, {"attr1": "value1", "attr2": "value2"}, 10)
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
        write_eap_span(span_ts, {"animal_type": "duck"}, 10)
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
            write_eap_span(span_ts, {"http.status_code": "200"}, 10)
        for i in range(10):
            span_ts = BASE_TIME - timedelta(seconds=i + 1)
            write_eap_span(span_ts, {"http.status_code": "502"}, 1)

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=12)).timestamp())
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
        write_eap_span(BASE_TIME, {"kyles_tag": "a", "const_1": 1}, 10)
        write_eap_span(BASE_TIME, {"kyles_tag": "b", "const_1": 1}, 5)
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=12)).timestamp())
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

    def test_span_id_column(self) -> None:
        write_eap_span(BASE_TIME)
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=12)).timestamp())
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
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.span_id"
                    )
                ),
            ],
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="sentry.span_id"
                    ),
                    op=ComparisonFilter.OP_EQUALS,
                    value=AttributeValue(val_str="123456781234567d"),
                )
            ),
        )
        response = EndpointTraceItemTable().execute(message)
        assert response.column_values == [
            TraceItemColumnValues(
                attribute_name="sentry.span_id",
                results=[AttributeValue(val_str="123456781234567d")],
            )
        ]

    def test_literal(self) -> None:
        span_ts = BASE_TIME - timedelta(minutes=1)
        write_eap_span(span_ts, {"measurement": 2}, 10)
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
        write_eap_span(span_ts, {"attr1": "1"}, 10)
        write_eap_span(span_ts, {"attr2": "2"}, 5)
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


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemTableEAPItems(TestTraceItemTable):
    """
    Run the tests again, but this time on the eap_items table as well to ensure it also works.
    """

    @pytest.fixture(autouse=True)
    def use_eap_items_table(
        self, snuba_set_config: SnubaSetConfig, redis_db: None
    ) -> None:
        snuba_set_config("use_eap_items_table", True)
        snuba_set_config("use_eap_items_table_start_timestamp_seconds", 0)

    def test_empty_downsampling_storage_config_does_not_have_downsampled_storage_meta(
        self,
    ) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = [
            gen_message(
                msg_timestamp,
            )
            for _ in range(30)
        ]
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())

        empty_downsampled_storage_config_message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(),
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color"))
            ],
        )
        response = EndpointTraceItemTable().execute(
            empty_downsampled_storage_config_message
        )
        assert not response.meta.HasField("downsampled_storage_meta")

    def test_preflight(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = [
            gen_message(
                msg_timestamp,
                tags={"preflighttag": "preflight"},
                randomize_span_id=True,
            )
            for _ in range(3600)
        ]
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())

        columns = [
            Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="preflighttag"))
        ]
        preflight_message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_PREFLIGHT
                ),
            ),
            columns=columns,
        )

        message_to_non_downsampled_tier = TraceItemTableRequest(
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
            columns=[
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="preflighttag")
                )
            ],
        )

        preflight_response = EndpointTraceItemTable().execute(preflight_message)
        non_downsampled_tier_response = EndpointTraceItemTable().execute(
            message_to_non_downsampled_tier
        )
        assert (
            len(preflight_response.column_values[0].results)
            < len(non_downsampled_tier_response.column_values[0].results) / 10
        )
        assert (
            preflight_response.meta.downsampled_storage_meta
            == DownsampledStorageMeta(
                tier=DownsampledStorageMeta.SelectedTier.SELECTED_TIER_64
            )
        )

    def test_best_effort_route_to_tier_64(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = [
            gen_message(
                msg_timestamp,
                tags={"tier64tag": "tier64tag"},
                randomize_span_id=True,
            )
            for _ in range(3600)
        ]
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())

        columns = [
            Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="tier64tag"))
        ]

        # sends a best effort request and a non-downsampled request to ensure their responses are different
        best_effort_message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_BEST_EFFORT
                ),
            ),
            columns=columns,
        )
        message_to_non_downsampled_tier = TraceItemTableRequest(
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
            columns=columns,
        )
        # this forces the query to route to tier 64. take a look at _get_target_tier to find out why
        with patch(
            "snuba.web.rpc.v1.resolvers.R_eap_items.routing_strategies.linear_bytes_scanned_storage_routing.LinearBytesScannedRoutingStrategy._get_query_bytes_scanned",
            return_value=20132659201,
        ):
            best_effort_response = EndpointTraceItemTable().execute(best_effort_message)
            non_downsampled_tier_response = EndpointTraceItemTable().execute(
                message_to_non_downsampled_tier
            )

            # tier 1's results should be 3600, so tier 64's results should be around 3600 / 64 (give or take due to random sampling)
            assert (
                len(non_downsampled_tier_response.column_values[0].results) / 200
                <= len(best_effort_response.column_values[0].results)
                <= len(non_downsampled_tier_response.column_values[0].results) / 16
            )
            assert (
                best_effort_response.meta.downsampled_storage_meta
                == DownsampledStorageMeta(
                    tier=DownsampledStorageMeta.SelectedTier.SELECTED_TIER_64
                )
            )

    def test_best_effort_end_to_end(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = [
            gen_message(
                msg_timestamp,
                tags={"endtoend": "endtoend"},
                randomize_span_id=True,
            )
            for _ in range(3600)
        ]
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())

        best_effort_message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_BEST_EFFORT
                ),
            ),
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="endtoend"))
            ],
        )
        response = EndpointTraceItemTable().execute(best_effort_message)
        assert (
            response.meta.downsampled_storage_meta.tier
            != DownsampledStorageMeta.SELECTED_TIER_UNSPECIFIED
        )

    def test_downsampling_uses_hexintcolumnprocessor(self) -> None:
        items_storage = get_storage(StorageKey("eap_items"))
        msg_timestamp = BASE_TIME - timedelta(minutes=1)
        messages = [
            gen_message(
                msg_timestamp,
                tags={"endtoend": "endtoend"},
                randomize_span_id=True,
            )
            for _ in range(3600)
        ]
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())

        best_effort_message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
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
        write_eap_span(span_ts, {"env": "prod", "num_cats": 1})
        write_eap_span(span_ts, {"env": "dev", "num_cats": 2})
        write_eap_span(span_ts, {"num_cats": 3})

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
