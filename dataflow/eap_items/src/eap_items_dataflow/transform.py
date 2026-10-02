"""Pure TraceItem -> BigQuery row conversion.

Everything in this module is side-effect free (no Beam, no I/O) so it can be
unit tested directly. Semantics mirror Snuba's EAP items consumer
(``snuba/rust_snuba/src/processors/eap_items.rs``) unless noted otherwise in
README.md ("Row semantics").
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import math
import uuid
from dataclasses import dataclass
from typing import Any

from google.protobuf.message import DecodeError
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, TraceItem

# Mirrors SAMPLING_FACTOR_PRECISION / MIN_SAMPLING_FACTOR in eap_items.rs.
SAMPLING_FACTOR_PRECISION = 1e6
MIN_SAMPLING_FACTOR = 1.0 / SAMPLING_FACTOR_PRECISION

# Mirrors RetentionKind::Standard defaults in rust_snuba/src/processors/utils.rs
# (Snuba can override these at runtime via sentry-options; we expose them as
# pipeline options instead).
DEFAULT_RETENTION_DAYS = 30
MAX_RETENTION_DAYS = 90

# Attribute Snuba adds from TraceItem.received (eap_items.rs, process_eap_item).
RECEIVED_AT_ATTRIBUTE = "sentry._internal.received_at"

_U64 = 1 << 64


class InvalidTraceItem(ValueError):
    """The payload cannot be turned into a row. Goes to the dead-letter output."""


@dataclass(frozen=True)
class TransformConfig:
    # Pipeline-side sampling rate in (0, 1]. 1.0 keeps everything.
    sample_rate: float = 1.0
    # Salt for the trace_id hash. Changing it selects a different, independent
    # subset of traces for the same sample_rate.
    sample_seed: str = ""
    retention_default_days: int = DEFAULT_RETENTION_DAYS
    retention_max_days: int = MAX_RETENTION_DAYS

    def __post_init__(self) -> None:
        if not (0.0 < self.sample_rate <= 1.0) or math.isnan(self.sample_rate):
            raise ValueError(f"sample_rate must be in (0, 1], got {self.sample_rate!r}")
        if self.retention_default_days <= 0 or self.retention_max_days <= 0:
            raise ValueError("retention_default_days and retention_max_days must be > 0")
        # blake2b keys are limited to 64 bytes; a longer seed would make every
        # item fail at runtime (and be dead-lettered) instead of at launch.
        if len(self.sample_seed.encode("utf-8")) > hashlib.blake2b.MAX_KEY_SIZE:
            raise ValueError(
                f"sample_seed must be at most {hashlib.blake2b.MAX_KEY_SIZE} bytes (UTF-8)"
            )


# --------------------------------------------------------------------------- ids


def format_item_id(raw: bytes) -> str:
    """Format TraceItem.item_id the way Snuba returns ``sentry.item_id``.

    Ingest: ``read_item_id`` in eap_items.rs reads the *first 16 bytes* as a
    little-endian u128 (shorter payloads are an error) into the UInt128
    ``item_id`` column.

    Query: ``HexIntColumnProcessor(columns=[item_id], size=32)`` renders it as
    ``lower(leftPad(hex(item_id), if(length(hex) > 16, 32, 16), '0'))``, i.e.
    16 hex chars when the value fits in 64 bits (span ids) and 32 otherwise
    (UUID-based ids such as logs/errors). This equals the hex of the original
    id produced by sentry's ``hex_to_item_id`` / relay's ``uuid_to_item_id``.
    """
    if len(raw) < 16:
        raise InvalidTraceItem(f"item_id too short: {len(raw)} bytes, expected 16")
    value = int.from_bytes(raw[:16], "little")
    return format(value, "016x" if value < _U64 else "032x")


def normalize_trace_id(trace_id: str) -> str:
    """Parse trace_id as a UUID (as Snuba does with ``Uuid::parse_str``) and
    return it as 32 lowercase hex chars without dashes (the format Snuba's
    ``UUIDColumnProcessor`` returns)."""
    try:
        return uuid.UUID(trace_id).hex
    except (ValueError, TypeError, AttributeError) as e:
        raise InvalidTraceItem(f"invalid trace_id {trace_id!r}: {e}") from e


def item_type_name(value: int) -> str:
    """Proto enum name, e.g. ``TRACE_ITEM_TYPE_SPAN``.

    Values unknown to the pinned sentry-protos version are kept (not
    dead-lettered) as ``TRACE_ITEM_TYPE_<number>`` so a new item type does not
    stall the pipeline before sentry-protos is bumped.
    """
    try:
        return TraceItemType.Name(value)
    except ValueError:
        return f"TRACE_ITEM_TYPE_{value}"


# ---------------------------------------------------------------------- sampling


def _is_valid_sample_rate(rate: float) -> bool:
    # eap_items.rs is_valid_sample_rate: (0, 1]. NaN compares false -> invalid.
    return 0.0 < rate <= 1.0


def snuba_sampling_factor(client_sample_rate: float, server_sample_rate: float) -> float:
    """``sampling_factor`` exactly as eap_items.rs computes it.

    Each of client/server rate is multiplied in only if it is in (0, 1]
    (proto default 0.0 means "unset" -> 1.0), then rounded to 1e-6 and clamped
    to a minimum of 1e-6.
    """
    factor = 1.0
    if _is_valid_sample_rate(client_sample_rate):
        factor *= client_sample_rate
    if _is_valid_sample_rate(server_sample_rate):
        factor *= server_sample_rate
    # Rust f64::round rounds half away from zero; Python round() is banker's.
    factor = math.floor(factor * SAMPLING_FACTOR_PRECISION + 0.5) / SAMPLING_FACTOR_PRECISION
    return max(factor, MIN_SAMPLING_FACTOR)


def trace_sample_hash(trace_id_hex: str, seed: str = "") -> float:
    """Deterministic value in [0, 1) derived from the normalized trace id.

    Every item of a trace (spans, logs, errors, metrics, ...) hashes to the
    same value, so pipeline sampling keeps or drops whole traces.
    """
    digest = hashlib.blake2b(
        bytes.fromhex(trace_id_hex), digest_size=8, key=seed.encode("utf-8")
    ).digest()
    return int.from_bytes(digest, "big") / float(_U64)


def keep_trace(trace_id_hex: str, config: TransformConfig) -> bool:
    if config.sample_rate >= 1.0:
        return True
    return trace_sample_hash(trace_id_hex, config.sample_seed) < config.sample_rate


# -------------------------------------------------------------------- retention


def enforce_retention_days(value: int, config: TransformConfig) -> int:
    """``enforce_standard_retention`` from rust_snuba/src/processors/utils.rs:
    missing/0 -> default (30), otherwise capped at max (90).

    Important for BigQuery: a raw 0 would mean "keep forever" in eap.items.
    """
    if value <= 0:
        return config.retention_default_days
    return min(value, config.retention_max_days)


# ------------------------------------------------------------------- attributes


def _json_double(value: float) -> float | str:
    # JSON has no NaN/Infinity; BigQuery rejects them in JSON columns.
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return value


def any_value_to_json(value: AnyValue) -> Any:
    """Encode an AnyValue as a plain JSON value.

    string -> string, bool -> bool, int -> integer (full int64 precision),
    double -> number (NaN/Inf as strings), array -> list (recursive),
    kvlist -> object (recursive), bytes -> base64 string, unset -> null.
    """
    kind = value.WhichOneof("value")
    if kind is None:
        return None
    if kind == "string_value":
        return value.string_value
    if kind == "bool_value":
        return value.bool_value
    if kind == "int_value":
        return value.int_value
    if kind == "double_value":
        return _json_double(value.double_value)
    if kind == "array_value":
        return [any_value_to_json(v) for v in value.array_value.values]
    if kind == "kvlist_value":
        return {kv.key: any_value_to_json(kv.value) for kv in value.kvlist_value.values}
    if kind == "bytes_value":
        return base64.b64encode(value.bytes_value).decode("ascii")
    raise InvalidTraceItem(f"unsupported AnyValue kind {kind!r}")  # pragma: no cover


def attributes_to_json(item: TraceItem) -> str:
    attrs: dict[str, Any] = {k: any_value_to_json(v) for k, v in item.attributes.items()}
    # Snuba adds the proto `received` timestamp (seconds) as an int attribute.
    if item.HasField("received"):
        attrs[RECEIVED_AT_ATTRIBUTE] = item.received.seconds
    return json.dumps(attrs, sort_keys=True, separators=(",", ":"), allow_nan=False)


# ------------------------------------------------------------------------- rows


def _timestamp_rfc3339(item: TraceItem) -> str:
    if not item.HasField("timestamp"):
        raise InvalidTraceItem("Expected a timestamp")
    ts = item.timestamp
    try:
        when = dt.datetime.fromtimestamp(ts.seconds, tz=dt.UTC) + dt.timedelta(
            microseconds=ts.nanos // 1000
        )
    except (OverflowError, OSError, ValueError) as e:
        raise InvalidTraceItem(f"timestamp out of range: {ts.seconds}s") from e
    # isoformat always zero-pads the year; strftime("%Y") does not on glibc
    # (year 1 -> "1-01-01..."), which Timestamp.from_rfc3339 cannot parse and
    # would crash the (unguarded) Storage Write conversion step.
    return when.replace(tzinfo=None).isoformat(timespec="microseconds") + "Z"


def decode_trace_item(payload: bytes) -> TraceItem:
    if not payload:
        raise InvalidTraceItem("empty payload")
    item = TraceItem()
    try:
        item.ParseFromString(payload)
    except DecodeError as e:
        raise InvalidTraceItem(f"protobuf decode error: {e}") from e
    return item


def trace_item_to_row(item: TraceItem, config: TransformConfig) -> dict[str, Any] | None:
    """Convert a decoded TraceItem into an eap.items row.

    Returns None when the item is dropped by pipeline-side sampling. Raises
    InvalidTraceItem for items Snuba would also reject (bad trace_id, short
    item_id, missing timestamp).
    """
    trace_id = normalize_trace_id(item.trace_id)
    item_id = format_item_id(item.item_id)
    timestamp = _timestamp_rfc3339(item)

    if not keep_trace(trace_id, config):
        return None

    sample_rate = (
        snuba_sampling_factor(item.client_sample_rate, item.server_sample_rate) * config.sample_rate
    )

    return {
        "organization_id": str(item.organization_id),
        "project_id": str(item.project_id),
        "item_type": item_type_name(item.item_type),
        "timestamp": timestamp,
        "trace_id": trace_id,
        "item_id": item_id,
        "attributes": attributes_to_json(item),
        "retention_days": enforce_retention_days(item.retention_days, config),
        "sample_rate": sample_rate,
    }


def payload_to_row(payload: bytes, config: TransformConfig) -> dict[str, Any] | None:
    return trace_item_to_row(decode_trace_item(payload), config)
