import base64
import json
import math
import uuid

import pytest
from eap_items_dataflow.transform import (
    InvalidTraceItem,
    TransformConfig,
    any_value_to_json,
    enforce_retention_days,
    format_item_id,
    item_type_name,
    keep_trace,
    payload_to_row,
    snuba_sampling_factor,
    trace_item_to_row,
    trace_sample_hash,
)
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import (
    AnyValue,
    ArrayValue,
    KeyValue,
    KeyValueList,
    TraceItem,
)

TRACE_ID = "3c1bfa0b2c4a4b5e9d7f0a1b2c3d4e5f"
SPAN_ID = "a1b2c3d4e5f60718"
ALL = TransformConfig()


def hex_to_item_id(hex_string: str) -> bytes:
    # sentry/src/sentry/utils/eap.py
    return int(hex_string, 16).to_bytes(16, "little")


def make_item(**overrides) -> TraceItem:
    ts = Timestamp(seconds=1_735_689_600, nanos=123_456_789)  # 2025-01-01T00:00:00.123456789Z
    fields = {
        "organization_id": 1,
        "project_id": 42,
        "trace_id": TRACE_ID,
        "item_id": hex_to_item_id(SPAN_ID),
        "item_type": TraceItemType.TRACE_ITEM_TYPE_SPAN,
        "timestamp": ts,
        "retention_days": 90,
    }
    fields.update(overrides)
    return TraceItem(**fields)


# ----------------------------------------------------------------- item_id


def test_item_id_span_id_is_16_hex_chars():
    assert format_item_id(hex_to_item_id(SPAN_ID)) == SPAN_ID


def test_item_id_span_id_with_leading_zero_keeps_padding():
    assert format_item_id(hex_to_item_id("00000000000000ab")) == "00000000000000ab"


def test_item_id_uuid_is_32_hex_chars_and_matches_relay():
    u = uuid.UUID("0190a8e3-7b1c-7cde-8f00-112233445566")
    # relay uuid_to_item_id: id.as_u128().to_le_bytes()
    raw = u.int.to_bytes(16, "little")
    assert format_item_id(raw) == u.hex


def test_item_id_only_first_16_bytes_used():
    raw = hex_to_item_id(SPAN_ID) + b"\xff\xff"
    assert format_item_id(raw) == SPAN_ID


def test_item_id_too_short_is_invalid():
    with pytest.raises(InvalidTraceItem, match="too short"):
        format_item_id(b"\x01" * 8)


# ---------------------------------------------------------- sampling factor


@pytest.mark.parametrize(
    "client,server,expected",
    [
        (0.0, 0.0, 1.0),  # unset -> 1.0
        (0.5, 0.0, 0.5),
        (0.0, 0.25, 0.25),
        (0.5, 0.1, 0.05),
        (1.5, 0.5, 0.5),  # out of range client ignored
        (-1.0, 2.0, 1.0),
        (float("nan"), 0.5, 0.5),
        (0.1, 0.1, 0.01),  # 0.010000000000000002 rounded to 1e-6
        (1e-4, 1e-4, 1e-6),  # 1e-8 clamped to MIN_SAMPLING_FACTOR
        (0.0000015, 1.0, 0.000002),  # round half away from zero
    ],
)
def test_snuba_sampling_factor(client, server, expected):
    assert snuba_sampling_factor(client, server) == pytest.approx(expected, abs=1e-12)


def test_sample_rate_column_combines_snuba_factor_and_pipeline_rate():
    item = make_item(client_sample_rate=0.5, server_sample_rate=0.5)
    # Find a trace kept at 10%.
    config = TransformConfig(sample_rate=0.1)
    kept = next(
        uuid.UUID(int=i).hex for i in range(10_000) if keep_trace(uuid.UUID(int=i).hex, config)
    )
    item.trace_id = kept
    row = trace_item_to_row(item, config)
    assert row["sample_rate"] == pytest.approx(0.025)


# -------------------------------------------------------- pipeline sampling


def test_sampling_full_rate_keeps_everything():
    assert all(keep_trace(uuid.uuid4().hex, ALL) for _ in range(1000))


def test_sampling_is_deterministic_per_trace_and_across_item_types():
    config = TransformConfig(sample_rate=0.3)
    for i in range(500):
        tid = uuid.UUID(int=i * 7919).hex
        decisions = set()
        for item_type in (TraceItemType.TRACE_ITEM_TYPE_SPAN, TraceItemType.TRACE_ITEM_TYPE_LOG):
            item = make_item(trace_id=tid, item_type=item_type, item_id=uuid.uuid4().bytes)
            decisions.add(trace_item_to_row(item, config) is None)
        assert len(decisions) == 1


def test_sampling_dashed_and_undashed_trace_ids_agree():
    config = TransformConfig(sample_rate=0.5)
    for _ in range(200):
        u = uuid.uuid4()
        a = trace_item_to_row(make_item(trace_id=u.hex), config)
        b = trace_item_to_row(make_item(trace_id=str(u).upper()), config)
        assert (a is None) == (b is None)


def test_sampling_rate_is_approximately_honored():
    config = TransformConfig(sample_rate=0.1)
    n = 50_000
    kept = sum(keep_trace(uuid.UUID(int=i * 2654435761).hex, config) for i in range(n))
    assert abs(kept / n - 0.1) < 0.01


def test_sampling_seed_changes_selection():
    tid = TRACE_ID
    assert trace_sample_hash(tid, "a") != trace_sample_hash(tid, "b")
    assert 0.0 <= trace_sample_hash(tid) < 1.0


@pytest.mark.parametrize("rate", [0.0, -0.1, 1.1, float("nan")])
def test_invalid_pipeline_sample_rate(rate):
    with pytest.raises(ValueError):
        TransformConfig(sample_rate=rate)


# ---------------------------------------------------------------- retention


@pytest.mark.parametrize("value,expected", [(0, 30), (1, 1), (30, 30), (90, 90), (400, 90)])
def test_retention(value, expected):
    assert enforce_retention_days(value, ALL) == expected


def test_retention_custom_bounds():
    config = TransformConfig(retention_default_days=7, retention_max_days=400)
    assert enforce_retention_days(0, config) == 7
    assert enforce_retention_days(396, config) == 396


# --------------------------------------------------------------- attributes


def test_any_value_encoding():
    assert any_value_to_json(AnyValue(string_value="x")) == "x"
    assert any_value_to_json(AnyValue(bool_value=False)) is False
    assert any_value_to_json(AnyValue(int_value=2**63 - 1)) == 2**63 - 1
    assert any_value_to_json(AnyValue(double_value=1.5)) == 1.5
    assert any_value_to_json(AnyValue(double_value=float("nan"))) == "NaN"
    assert any_value_to_json(AnyValue(double_value=float("-inf"))) == "-Infinity"
    assert (
        any_value_to_json(AnyValue(bytes_value=b"\x00\xff"))
        == base64.b64encode(b"\x00\xff").decode()
    )
    assert any_value_to_json(AnyValue()) is None
    nested = AnyValue(
        array_value=ArrayValue(
            values=[
                AnyValue(int_value=1),
                AnyValue(string_value="a"),
                AnyValue(array_value=ArrayValue(values=[AnyValue(bool_value=True)])),
            ]
        )
    )
    assert any_value_to_json(nested) == [1, "a", [True]]
    kv = AnyValue(
        kvlist_value=KeyValueList(
            values=[
                KeyValue(key="k", value=AnyValue(double_value=2.0)),
                KeyValue(key="empty", value=AnyValue()),
            ]
        )
    )
    assert any_value_to_json(kv) == {"k": 2.0, "empty": None}


def test_attributes_json_and_received_at():
    item = make_item(
        attributes={
            "sentry.op": AnyValue(string_value="http.server"),
            "sentry.duration_ms": AnyValue(double_value=12.5),
            "count": AnyValue(int_value=3),
            "flag": AnyValue(bool_value=True),
            "weird": AnyValue(double_value=float("inf")),
        },
        received=Timestamp(seconds=1_735_689_601, nanos=999),
    )
    row = trace_item_to_row(item, ALL)
    attrs = json.loads(row["attributes"])
    assert attrs == {
        "sentry.op": "http.server",
        "sentry.duration_ms": 12.5,
        "count": 3,
        "flag": True,
        "weird": "Infinity",
        "sentry._internal.received_at": 1_735_689_601,
    }


# --------------------------------------------------------------------- rows


def test_full_row():
    item = make_item(
        organization_id=2**64 - 1,
        project_id=4_505_000_000_000_001,
        trace_id=str(uuid.UUID(TRACE_ID)),
        client_sample_rate=0.5,
        attributes={"a": AnyValue(string_value="b")},
    )
    row = payload_to_row(item.SerializeToString(), ALL)
    assert row == {
        "organization_id": "18446744073709551615",
        "project_id": "4505000000000001",
        "item_type": "TRACE_ITEM_TYPE_SPAN",
        "timestamp": "2025-01-01T00:00:00.123456Z",
        "trace_id": TRACE_ID,
        "item_id": SPAN_ID,
        "attributes": '{"a":"b"}',
        "retention_days": 90,
        "sample_rate": 0.5,
    }


def test_item_type_names():
    assert item_type_name(TraceItemType.TRACE_ITEM_TYPE_LOG) == "TRACE_ITEM_TYPE_LOG"
    assert item_type_name(0) == "TRACE_ITEM_TYPE_UNSPECIFIED"
    assert item_type_name(999) == "TRACE_ITEM_TYPE_999"


def test_empty_attributes_is_empty_object():
    assert trace_item_to_row(make_item(), ALL)["attributes"] == "{}"


@pytest.mark.parametrize(
    "payload,match",
    [
        (b"", "empty payload"),
        (b"\xff\xff\xff\xff", "protobuf decode error"),
    ],
)
def test_undecodable_payloads(payload, match):
    with pytest.raises(InvalidTraceItem, match=match):
        payload_to_row(payload, ALL)


@pytest.mark.parametrize(
    "overrides,match",
    [
        ({"trace_id": ""}, "invalid trace_id"),
        ({"trace_id": "not-a-uuid"}, "invalid trace_id"),
        ({"item_id": b""}, "too short"),
    ],
)
def test_invalid_items(overrides, match):
    with pytest.raises(InvalidTraceItem, match=match):
        trace_item_to_row(make_item(**overrides), ALL)


def test_missing_timestamp_is_invalid():
    item = make_item()
    item.ClearField("timestamp")
    with pytest.raises(InvalidTraceItem, match="timestamp"):
        trace_item_to_row(item, ALL)


def test_json_output_has_no_nan():
    item = make_item(attributes={"x": AnyValue(double_value=math.nan)})
    json.loads(trace_item_to_row(item, ALL)["attributes"], parse_constant=lambda c: pytest.fail(c))


def test_timestamp_year_is_zero_padded_and_parseable():
    # strftime("%Y") does not zero-pad on glibc; the Storage Write step parses
    # this string with Timestamp.from_rfc3339 outside the dead-letter guard.
    from eap_items_dataflow.pipeline import to_storage_write_row

    item = make_item(timestamp=Timestamp(seconds=-62135596800, nanos=5000))  # 0001-01-01
    ts = trace_item_to_row(item, ALL)["timestamp"]
    assert ts == "0001-01-01T00:00:00.000005Z"
    row = to_storage_write_row(trace_item_to_row(item, ALL))
    assert row["timestamp"].micros == -62135596800 * 10**6 + 5


def test_sample_seed_longer_than_blake2b_key_is_rejected_at_config_time():
    TransformConfig(sample_rate=0.5, sample_seed="x" * 64)
    with pytest.raises(ValueError, match="sample_seed"):
        TransformConfig(sample_rate=0.5, sample_seed="x" * 65)
