import uuid
from datetime import timedelta
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_options.testing import override_options
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
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, ArrayValue

from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_details import (
    EndpointTraceItemDetails,
    _convert_results,
)
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import (
    BASE_TIME,
    END_TIMESTAMP,
    START_TIMESTAMP,
    gen_item_message,
)

_REQUEST_ID = uuid.uuid4().hex
_TRACE_ID = str(uuid.uuid4())


@pytest.fixture(autouse=False)
def setup_logs_in_db(eap: None, redis_db: None) -> None:
    logs_storage = get_writable_storage(StorageKey("eap_items"))
    messages = []
    for i in range(120):
        timestamp = BASE_TIME + timedelta(minutes=i)
        timestamp_nanos = int(timestamp.timestamp() * 1e9)
        messages.append(
            gen_item_message(
                start_timestamp=timestamp,
                remove_default_attributes=True,
                type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                attributes={
                    "bool_tag": AnyValue(bool_value=i % 2 == 0),
                    "double_tag": AnyValue(double_value=1234567890.123),
                    "int_tag": AnyValue(int_value=i),
                    "observed_timestamp_nanos": AnyValue(int_value=timestamp_nanos),
                    "sentry.body": AnyValue(string_value=f"hello world {i}"),
                    "sentry.severity_number": AnyValue(int_value=10),
                    "sentry.severity_text": AnyValue(string_value="info"),
                    "sentry.timestamp_precise": AnyValue(int_value=timestamp_nanos),
                    "span_id": AnyValue(string_value="123456781234567D"),
                    "str_tag": AnyValue(string_value=f"num: {i}"),
                    "timestamp_nanos": AnyValue(int_value=timestamp_nanos),
                },
            )
        )
    write_raw_unprocessed_events(logs_storage, messages)


@pytest.fixture(autouse=False)
def setup_spans_in_db(eap: None, redis_db: None) -> None:
    spans_storage = get_writable_storage(StorageKey("eap_items"))
    messages = [
        gen_item_message(
            start_timestamp=BASE_TIME - timedelta(minutes=i),
            attributes={
                "str_tag": AnyValue(string_value=f"num: {i}"),
                "double_tag": AnyValue(double_value=1234567890.123),
                "sentry.segment_id": AnyValue(string_value=uuid.uuid4().hex[:16]),
            },
        )
        for i in range(120)
    ]

    write_raw_unprocessed_events(spans_storage, messages)


def _str_tags_array(*values: str) -> AnyValue:
    return AnyValue(array_value=ArrayValue(values=[AnyValue(string_value=v) for v in values]))


def _int_vals_array(*values: int) -> AnyValue:
    return AnyValue(array_value=ArrayValue(values=[AnyValue(int_value=v) for v in values]))


@pytest.mark.eap
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
            trace_id=uuid.uuid4().hex,
        )
        response = self.app.post(
            "/rpc/EndpointTraceItemDetails/v1", data=message.SerializeToString()
        )
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 404, error_proto

    def test_missing_item_id(self, setup_logs_in_db: Any) -> None:
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
            trace_id=uuid.uuid4().hex,
        )
        response = self.app.post(
            "/rpc/EndpointTraceItemDetails/v1", data=message.SerializeToString()
        )
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 400, error_proto

    def test_missing_trace_id(self, setup_logs_in_db: Any) -> None:
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
        assert response.status_code == 400, error_proto

    def test_invalid_trace_id(self, setup_logs_in_db: Any) -> None:
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
            trace_id="baduuid",
        )
        response = self.app.post(
            "/rpc/EndpointTraceItemDetails/v1", data=message.SerializeToString()
        )
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 400, error_proto

    def test_endpoint_on_logs(self, setup_logs_in_db: Any) -> None:
        logs = (
            EndpointTraceItemTable()
            .execute(
                TraceItemTableRequest(
                    meta=RequestMeta(
                        project_ids=[1],
                        organization_id=1,
                        cogs_category="something",
                        referrer="something",
                        start_timestamp=START_TIMESTAMP,
                        end_timestamp=END_TIMESTAMP,
                        request_id=_REQUEST_ID,
                        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                    ),
                    columns=[
                        Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")
                        ),
                        Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.trace_id")
                        ),
                    ],
                )
            )
            .column_values
        )
        log_id = logs[0].results[0].val_str
        trace_id = logs[1].results[0].val_str

        res = EndpointTraceItemDetails().execute(
            TraceItemDetailsRequest(
                meta=RequestMeta(
                    project_ids=[1],
                    organization_id=1,
                    cogs_category="something",
                    referrer="something",
                    start_timestamp=START_TIMESTAMP,
                    end_timestamp=END_TIMESTAMP,
                    request_id=_REQUEST_ID,
                    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                ),
                item_id=log_id,
                trace_id=trace_id,
            )
        )
        attributes_returned = {x.name for x in res.attributes}

        for k in {
            "sentry.body",
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
        }:
            assert k in attributes_returned, k

    def test_endpoint_returns_array_attribute(self, eap: None, redis_db: None) -> None:
        """Allowlisted attributes_array paths are exposed as val_array on TraceItemDetails."""
        span_ts = BASE_TIME - timedelta(minutes=1)
        storage = get_writable_storage(StorageKey("eap_items"))
        write_raw_unprocessed_events(
            storage,
            [
                gen_item_message(
                    span_ts,
                    attributes={
                        "gen_ai.response.text": _str_tags_array("gamma", "delta"),
                        "workflow_ids": _int_vals_array(1, 3),
                    },
                ),
            ],
        )
        start = Timestamp()
        end = Timestamp()
        start.FromDatetime(BASE_TIME - timedelta(hours=4))
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
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")
                        ),
                        Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.trace_id")
                        ),
                    ],
                )
            )
            .column_values
        )
        item_id = spans[0].results[0].val_str
        trace_id = spans[1].results[0].val_str

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
                item_id=item_id,
                trace_id=trace_id,
            )
        )
        tags_attr = next((a for a in res.attributes if a.name == "gen_ai.response.text"), None)
        assert tags_attr is not None
        assert tags_attr.value.WhichOneof("value") == "val_array"
        assert [e.val_str for e in tags_attr.value.val_array.values] == ["gamma", "delta"]
        cols_attr = next((a for a in res.attributes if a.name == "workflow_ids"), None)
        assert cols_attr is not None
        assert cols_attr.value.WhichOneof("value") == "val_array"
        assert [e.val_int for e in cols_attr.value.val_array.values] == [1, 3]

    @pytest.mark.parametrize(
        "read_from_typed_columns",
        [True, False],
        ids=["after_cutoff_typed_columns", "before_cutoff_json_allowlist"],
    )
    def test_array_attributes_before_and_after_cutoff(
        self, eap: None, redis_db: None, read_from_typed_columns: bool
    ) -> None:
        """An allowlisted array attribute decodes to the same val_array whether read from
        the typed columns (window on/after the cutoff) or the legacy JSON-column allowlist
        (before it) — the data is double-written. Past the cutoff a NON-allowlisted array
        attribute is also returned, since the typed-column read drops the allowlist."""
        span_ts = BASE_TIME - timedelta(minutes=1)
        storage = get_storage(StorageKey("eap_items"))
        write_raw_unprocessed_events(
            storage,  # type: ignore
            [
                gen_item_message(
                    span_ts,
                    attributes={
                        # allowlisted -> returned by both read paths
                        "gen_ai.response.text": _str_tags_array("gamma", "delta"),
                        # not allowlisted -> only the typed-column read path returns it
                        "my_tags": _str_tags_array("alpha", "beta"),
                    },
                ),
            ],
        )
        start = Timestamp()
        end = Timestamp()
        start.FromDatetime(BASE_TIME - timedelta(hours=4))
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
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")
                        ),
                        Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.trace_id")
                        ),
                    ],
                )
            )
            .column_values
        )
        item_id = spans[0].results[0].val_str
        trace_id = spans[1].results[0].val_str

        # 0 disables the typed-column read path (legacy JSON allowlist); a low value
        # enables it for the (recent) request window.
        with override_options(
            "snuba",
            {"use_array_map_columns_timestamp_seconds": 10 if read_from_typed_columns else 0},
        ):
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
                    item_id=item_id,
                    trace_id=trace_id,
                )
            )
        by_name = {a.name: a.value for a in res.attributes}
        # Allowlisted array attribute: identical val_array from both read paths.
        assert by_name["gen_ai.response.text"].WhichOneof("value") == "val_array"
        assert [e.val_str for e in by_name["gen_ai.response.text"].val_array.values] == [
            "gamma",
            "delta",
        ]
        if read_from_typed_columns:
            # Allowlist dropped: a non-allowlisted array attribute is now returned too.
            assert "my_tags" in by_name
            assert [e.val_str for e in by_name["my_tags"].val_array.values] == ["alpha", "beta"]
        else:
            # Legacy JSON allowlist: a non-allowlisted array attribute is omitted.
            assert "my_tags" not in by_name

    def test_dotted_key_array_attribute_parsed_properly(self, eap: None, redis_db: None) -> None:
        trace_id = uuid.uuid4().hex
        span_ts = BASE_TIME - timedelta(minutes=1)
        storage = get_writable_storage(StorageKey("eap_items"))
        write_raw_unprocessed_events(
            storage,
            [
                gen_item_message(
                    span_ts,
                    trace_id=trace_id,
                    remove_default_attributes=True,
                    attributes={
                        "gen_ai.input.messages": _str_tags_array("node", "--enable-source-maps"),
                    },
                ),
            ],
        )
        start = Timestamp()
        end = Timestamp()
        start.FromDatetime(BASE_TIME - timedelta(hours=4))
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
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")
                        ),
                        Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.trace_id")
                        ),
                    ],
                )
            )
            .column_values
        )
        idx = next(
            i
            for i, tr in enumerate(spans[1].results)
            if tr.val_str.replace("-", "").lower() == trace_id.replace("-", "").lower()
        )
        item_id = spans[0].results[idx].val_str
        trace_id_for_details = spans[1].results[idx].val_str

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
                item_id=item_id,
                trace_id=trace_id_for_details,
            )
        )
        attr = next((a for a in res.attributes if a.name == "gen_ai.input.messages"), None)
        assert attr is not None
        assert attr.value.WhichOneof("value") == "val_array"
        assert [e.val_str for e in attr.value.val_array.values] == ["node", "--enable-source-maps"]

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
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.item_id")
                        ),
                        Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.trace_id")
                        ),
                    ],
                )
            )
            .column_values
        )
        span_id = spans[0].results[0].val_str
        trace_id = spans[1].results[0].val_str

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
                trace_id=trace_id,
            )
        )
        attributes_returned = {x.name for x in res.attributes}

        for k in {
            "double_tag",
            "sentry.duration_ms",
            "sentry.end_timestamp_precise",
            "sentry.event_id",
            "sentry.exclusive_time_ms",
            "sentry.is_segment",
            "sentry.item_type",
            "sentry.organization_id",
            "sentry.project_id",
            "sentry.raw_description",
            "sentry.received",
            "sentry.segment_id",
            "sentry.start_timestamp_precise",
            "sentry.trace_id",
            "str_tag",
        }:
            assert k in attributes_returned, k


def test_convert_results_dedupes() -> None:
    """
    Makes sure that _convert_results dedupes int/bool and float
    attributes. We store float versions of int and bool attributes
    for computational reasons but we don't want to return the
    duplicate float attrs to the user.
    """
    data = [
        {
            "timestamp": 1750964400,
            "hex_item_id": "e70ef5b1b5bc4611840eff9964b7a767",
            "trace_id": "cb190d6e7d5743d5bc1494c650592cd2",
            "organization_id": 1,
            "project_id": 1,
            "item_type": 1,
            "attributes_string": {
                "relay_protocol_version": "3",
                "sentry.segment_id": "30c64b1f21b54799",
            },
            "attributes_int": {"sentry.duration_ms": 152},
            "attributes_float": {"sentry.is_segment": 1.0, "num_of_spans": 50.0},
            "attributes_bool": {
                "my.true.bool.field": True,
                "sentry.is_segment": True,
                "my.false.bool.field": False,
            },
        }
    ]
    _, _, attrs = _convert_results(data, read_typed_arrays=False)
    is_segment_attrs = list(filter(lambda x: x.name == "sentry.is_segment", attrs))
    assert len(is_segment_attrs) == 1


def test_convert_results_includes_attributes_array() -> None:
    """
    TraceItemDetails maps per-path JSON sub-column payloads from `attributes_array`
    into val_array attributes (only allowlisted paths are read).
    """
    data = [
        {
            "timestamp": 1750964400,
            "hex_item_id": "e70ef5b1b5bc4611840eff9964b7a767",
            "trace_id": "cb190d6e7d5743d5bc1494c650592cd2",
            "organization_id": 1,
            "project_id": 1,
            "item_type": 1,
            "attributes_string": {},
            "attributes_int": {},
            "attributes_float": {},
            "attributes_bool": {},
            "gen_ai.input.messages": '[{"String":"gamma"},{"String":"delta"}]',
        }
    ]
    _, _, attrs = _convert_results(data, read_typed_arrays=False)
    msgs = [a for a in attrs if a.name == "gen_ai.input.messages"]
    assert len(msgs) == 1
    assert msgs[0].value.WhichOneof("value") == "val_array"
    assert [e.val_str for e in msgs[0].value.val_array.values] == ["gamma", "delta"]


def test_convert_results_reads_typed_array_maps() -> None:
    """Past the cutoff, every array attribute is read from the typed array map columns
    (not just an allowlist). Homogeneous arrays keep order; a mixed-type array is merged
    across the typed columns in column order (string, int, float, bool)."""
    data = [
        {
            "timestamp": 1750964400,
            "hex_item_id": "e70ef5b1b5bc4611840eff9964b7a767",
            "trace_id": "cb190d6e7d5743d5bc1494c650592cd2",
            "organization_id": 1,
            "project_id": 1,
            "item_type": 1,
            "attributes_string": {},
            "attributes_int": {},
            "attributes_float": {},
            "attributes_bool": {},
            "attributes_array_string": {"tags": ["a", "b"], "mixed": ["s"]},
            "attributes_array_int": {"cols": [1, 3], "mixed": [9]},
            "attributes_array_float": {"ratios": [1.5]},
            "attributes_array_bool": {"flags": [True, False]},
        }
    ]
    _, _, attrs = _convert_results(data, read_typed_arrays=True)
    by_name = {a.name: a.value for a in attrs}
    # Not restricted to an allowlist: arbitrary array attributes are returned.
    assert [e.val_str for e in by_name["tags"].val_array.values] == ["a", "b"]
    assert [e.val_int for e in by_name["cols"].val_array.values] == [1, 3]
    assert by_name["ratios"].val_array.values[0].WhichOneof("value") == "val_double"
    assert [e.val_bool for e in by_name["flags"].val_array.values] == [True, False]
    # A name present in multiple typed maps is merged (string elements then int).
    assert [
        (e.WhichOneof("value"), e.val_str or e.val_int) for e in by_name["mixed"].val_array.values
    ] == [("val_str", "s"), ("val_int", 9)]


def test_convert_results_skips_non_allowlisted_array_paths() -> None:
    """Rows only contain keys we explicitly selected; anything else is ignored."""
    data = [
        {
            "timestamp": 1750964400,
            "hex_item_id": "e70ef5b1b5bc4611840eff9964b7a767",
            "trace_id": "cb190d6e7d5743d5bc1494c650592cd2",
            "organization_id": 1,
            "project_id": 1,
            "item_type": 1,
            "attributes_string": {},
            "attributes_int": {},
            "attributes_float": {},
            "attributes_bool": {},
            "gen_ai.input.messages": "[]",
        }
    ]
    _, _, attrs = _convert_results(data, read_typed_arrays=False)
    assert not any(a.name == "gen_ai.input.messages" for a in attrs)
