import math
import uuid
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_get_traces_pb2 import GetTracesResponse
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ComparisonFilter,
    OrFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, ArrayValue, TraceItem

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.helpers import write_raw_unprocessed_events

RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"
SERVER_NAME = "D23CXQ4GK2.local"
BASE_TIME = datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0) - timedelta(minutes=180)
START_TIMESTAMP = Timestamp(seconds=int((BASE_TIME - timedelta(hours=3)).timestamp()))
END_TIMESTAMP = Timestamp(seconds=int((BASE_TIME + timedelta(hours=3)).timestamp()))

_DEFAULT_ATTRIBUTES = {
    "category": AnyValue(string_value="http"),
    "description": AnyValue(string_value="/api/0/relays/projectconfigs/"),
    "environment": AnyValue(string_value="development"),
    "http.status_code": AnyValue(string_value="200"),
    "my.false.bool.field": AnyValue(bool_value=False),
    "my.float.field": AnyValue(double_value=101.2),
    "my.int.field": AnyValue(int_value=2000),
    "my.neg.field": AnyValue(int_value=-100),
    "my.neg.float.field": AnyValue(double_value=-101.2),
    "my.numeric.attribute": AnyValue(int_value=1),
    "my.true.bool.field": AnyValue(bool_value=True),
    "num_of_spans": AnyValue(double_value=50.0),
    "op": AnyValue(string_value="http.server"),
    "origin": AnyValue(string_value="auto.http.django"),
    "platform": AnyValue(string_value="python"),
    "relay_endpoint_version": AnyValue(string_value="3"),
    "relay_id": AnyValue(string_value="88888888-4444-4444-8444-cccccccccccc"),
    "relay_no_cache": AnyValue(string_value="False"),
    "relay_protocol_version": AnyValue(string_value="3"),
    "relay_use_post_or_schedule": AnyValue(string_value="True"),
    "relay_use_post_or_schedule_rejected": AnyValue(string_value="version"),
    "sdk.name": AnyValue(string_value="sentry.python.django"),
    "sdk.version": AnyValue(string_value="2.7.0"),
    "sentry.category": AnyValue(string_value="some"),
    "sentry.duration_ms": AnyValue(int_value=152),
    "sentry.environment": AnyValue(string_value="development"),
    "sentry.event_id": AnyValue(string_value="d826225de75d42d6b2f01b957d51f18f"),
    "sentry.exclusive_time_ms": AnyValue(double_value=0.228),
    "sentry.is_segment": AnyValue(bool_value=True),
    "sentry.raw_description": AnyValue(string_value="/api/0/relays/projectconfigs/"),
    "sentry.release": AnyValue(string_value=RELEASE_TAG),
    "sentry.sdk.name": AnyValue(string_value="sentry.python.django"),
    "sentry.sdk.version": AnyValue(string_value="2.7.0"),
    "sentry.segment.name": AnyValue(string_value="/api/0/relays/projectconfigs/"),
    "server_name": AnyValue(string_value=SERVER_NAME),
    "spans_over_limit": AnyValue(string_value="False"),
    "status": AnyValue(string_value="ok"),
    "status_code": AnyValue(string_value="200"),
    "thread.id": AnyValue(string_value="8522009600"),
    "thread.name": AnyValue(string_value="uWSGIWorker1Core0"),
    "trace.status": AnyValue(string_value="ok"),
    "transaction": AnyValue(string_value="/api/0/relays/projectconfigs/"),
    "transaction.method": AnyValue(string_value="POST"),
    "transaction.op": AnyValue(string_value="http.server"),
    "user": AnyValue(string_value="ip:127.0.0.1"),
    "i_am_an_array": AnyValue(
        array_value=ArrayValue(
            values=[
                AnyValue(int_value=1),
                AnyValue(bool_value=True),
                AnyValue(double_value=3.0),
                AnyValue(string_value="blah"),
            ]
        )
    ),
}

# current UTC time rounded down to the start of the current hour, then minus 180 minutes.
BASE_TIME = datetime.now(tz=timezone.utc).replace(
    minute=0,
    second=0,
    microsecond=0,
) - timedelta(minutes=180)


def write_eap_item(
    start_timestamp: datetime,
    raw_attributes: dict[str, str | float | int | bool] = {},
    count: int = 1,
    server_sample_rate: float = 1.0,
    item_id: Optional[bytes] = None,
) -> None:
    """
    This is a helper function to write a single or multiple eap-spans to the database.
    It uses gen_message to generate the spans and then writes them to the database.

    Args:
        timestamp: The timestamp of the span to write.
        attributes: attributes to go on the span.
        count: the number of these spans to write.
    """

    def convert_attribute_value(value: Any) -> AnyValue:
        if isinstance(value, str):
            return AnyValue(string_value=value)
        elif isinstance(value, int):
            return AnyValue(int_value=value)
        elif isinstance(value, bool):
            return AnyValue(bool_value=value)
        elif isinstance(value, float):
            return AnyValue(double_value=value)
        elif isinstance(value, list):
            return AnyValue(
                array_value=ArrayValue(values=[convert_attribute_value(v) for v in value])
            )
        return AnyValue()

    attributes: dict[str, AnyValue] = {}
    for key, value in raw_attributes.items():
        attributes[key] = convert_attribute_value(value)

    write_raw_unprocessed_events(
        get_storage(StorageKey("eap_items")),  # type: ignore
        [
            gen_item_message(
                start_timestamp=start_timestamp,
                attributes=attributes,
                server_sample_rate=server_sample_rate,
                item_id=item_id,
            )
            for _ in range(count)
        ],
    )


def gen_item_message(
    start_timestamp: datetime,
    attributes: dict[str, AnyValue] = {},
    type: TraceItemType.ValueType = TraceItemType.TRACE_ITEM_TYPE_SPAN,
    trace_id: Optional[str] = None,
    server_sample_rate: float = 1.0,
    client_sample_rate: float = 1.0,
    end_timestamp: Optional[datetime] = None,
    remove_default_attributes: bool = False,
    item_id: Optional[bytes] = None,
    project_id: Optional[int] = None,
    organization_id: Optional[int] = None,
) -> bytes:
    item_timestamp = Timestamp()
    item_timestamp.FromDatetime(start_timestamp)
    received = Timestamp()
    received.GetCurrentTime()
    if end_timestamp is None:
        end_timestamp = start_timestamp + timedelta(seconds=1)
    if remove_default_attributes is False:
        attributes = _DEFAULT_ATTRIBUTES | attributes
    attributes.update(
        {
            "sentry.end_timestamp_precise": AnyValue(double_value=end_timestamp.timestamp()),
            "sentry.received": AnyValue(double_value=received.seconds),
            "sentry.start_timestamp_precise": AnyValue(double_value=start_timestamp.timestamp()),
            "start_timestamp_ms": AnyValue(double_value=(int(start_timestamp.timestamp() * 1000))),
        }
    )
    if item_id is None:
        item_id = uuid.uuid4().int.to_bytes(16, byteorder="little")
    if trace_id is None:
        trace_id = uuid.uuid4().hex
    return TraceItem(
        organization_id=organization_id or 1,
        project_id=project_id or 1,
        item_type=type,
        timestamp=item_timestamp,
        trace_id=trace_id,
        item_id=item_id,
        received=received,
        retention_days=90,
        server_sample_rate=server_sample_rate,
        attributes=attributes,
    ).SerializeToString()


def create_request_meta(
    start_timestamp: datetime,
    end_timestamp: datetime,
    trace_item_type: TraceItemType.ValueType = TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED,
) -> RequestMeta:
    return RequestMeta(
        project_ids=[1],
        organization_id=1,
        cogs_category="test",
        referrer="test",
        start_timestamp=Timestamp(seconds=int(start_timestamp.timestamp())),
        end_timestamp=Timestamp(seconds=int(end_timestamp.timestamp())),
        trace_item_type=trace_item_type,
        request_id=uuid.uuid4().hex,
    )


def comparison_filter(
    field_name: str,
    field_value: Any,
    op: ComparisonFilter.Op.ValueType = ComparisonFilter.Op.OP_EQUALS,
) -> TraceItemFilter:
    if isinstance(field_value, str):
        value = AttributeValue(val_str=field_value)
    elif isinstance(field_value, bool):
        value = AttributeValue(val_bool=field_value)
    elif isinstance(field_value, int):
        value = AttributeValue(val_int=field_value)
    elif isinstance(field_value, float):
        value = AttributeValue(val_double=field_value)
    else:
        raise ValueError(f"Unsupported field value type: {type(field_value)}")

    return TraceItemFilter(
        comparison_filter=ComparisonFilter(
            key=AttributeKey(name=field_name, type=AttributeKey.TYPE_STRING),
            op=op,
            value=value,
        ),
    )


def and_filter(filters: list[TraceItemFilter]) -> TraceItemFilter:
    return TraceItemFilter(
        and_filter=AndFilter(
            filters=filters,
        ),
    )


def or_filter(filters: list[TraceItemFilter]) -> TraceItemFilter:
    return TraceItemFilter(
        or_filter=OrFilter(
            filters=filters,
        ),
    )


def create_cross_item_test_data() -> tuple[list[str], list[bytes], datetime, datetime]:
    """
    Create test data with 6 traces. The first 3 traces have items with the following attributes:
    - span.attr1 = val1
    - log.attr2 = val2
    - error.attr3 = val3
    - error.attr4 = val4
    The last 3 traces have items with the following attributes:
    - span.attr1 = other_val1
    - log.attr2 = other_val2
    - error.attr3 = other_val3
    - error.attr4 = other_val4
    """
    today = datetime.now(tz=UTC).date()
    start_time = datetime.combine(today, datetime.min.time(), tzinfo=UTC)
    end_time = start_time + timedelta(hours=1)

    trace_ids = [uuid.uuid4().hex for _ in range(6)]
    all_items = []

    for i, trace_id in enumerate(trace_ids):
        item_time = start_time + timedelta(minutes=i * 10)

        if i < 3:
            span_attrs = {"span.attr1": AnyValue(string_value="val1")}
            log_attrs = {"log.attr2": AnyValue(string_value="val2")}
            error_attrs = {
                "error.attr3": AnyValue(string_value="val3"),
                "error.attr4": AnyValue(string_value="val4"),
            }
        else:
            span_attrs = {"span.attr1": AnyValue(string_value="other_val1")}
            log_attrs = {"log.attr2": AnyValue(string_value="other_val2")}
            error_attrs = {
                "error.attr3": AnyValue(string_value="other_val3"),
                "error.attr4": AnyValue(string_value="other_val4"),
            }

        # Create span item
        all_items.append(
            gen_item_message(
                start_timestamp=item_time,
                trace_id=trace_id,
                type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                attributes=span_attrs,
                remove_default_attributes=False,
            )
        )

        # Create log item
        all_items.append(
            gen_item_message(
                start_timestamp=item_time + timedelta(seconds=10),
                trace_id=trace_id,
                type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                attributes=log_attrs,
                remove_default_attributes=False,
            )
        )

        # Create error item
        all_items.append(
            gen_item_message(
                start_timestamp=item_time + timedelta(seconds=20),
                trace_id=trace_id,
                type=TraceItemType.TRACE_ITEM_TYPE_ERROR,
                attributes=error_attrs,
                remove_default_attributes=False,
            )
        )

    return trace_ids, all_items, start_time, end_time


def write_cross_item_data_to_storage(items: list[bytes]) -> None:
    """Write cross-item test data to storage."""
    from snuba.datasets.storages.factory import get_storage
    from snuba.datasets.storages.storage_key import StorageKey
    from tests.helpers import write_raw_unprocessed_events

    storage = get_storage(StorageKey("eap_items"))
    write_raw_unprocessed_events(storage, items)  # type: ignore


def _compare_values(
    expected: Any,
    actual: Any,
    path: str,
    differences: List[Tuple[str, Any, Any]],
) -> None:
    """
    Recursively compare two values and track differences.

    Handles primitives, lists (repeated fields), dicts (nested messages), and special cases
    like float precision and None vs empty list equivalence.

    Args:
        expected: Expected value
        actual: Actual value
        path: Dot-notation path to current field (e.g., "traces[0].attributes[1].value")
        differences: List to accumulate (path, expected, actual) tuples
    """
    # Handle None values
    if expected is None and actual is None:
        return

    # Treat empty list as equivalent to None (protobuf behavior)
    if (expected is None or expected == []) and (actual is None or actual == []):
        return

    # Treat empty dict as equivalent to None (protobuf behavior for missing nested messages)
    if (expected is None or expected == {}) and (actual is None or actual == {}):
        return

    # Handle case where one is None but not the other (after empty list/dict check)
    if expected is None and actual is not None:
        differences.append((path, "not present", actual))
        return
    if actual is None and expected is not None:
        differences.append((path, expected, "not present"))
        return

    # Handle lists (repeated fields)
    if isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            differences.append(
                (f"{path} (length)", f"{len(expected)} item(s)", f"{len(actual)} item(s)")
            )
            # Still compare the overlapping elements

        for i, (exp_item, act_item) in enumerate(zip(expected, actual)):
            _compare_values(exp_item, act_item, f"{path}[{i}]", differences)

        # If lengths differ, note any extra items in the longer list
        if len(expected) > len(actual):
            for i in range(len(actual), len(expected)):
                differences.append((f"{path}[{i}]", expected[i], "not present"))
        elif len(actual) > len(expected):
            for i in range(len(expected), len(actual)):
                differences.append((f"{path}[{i}]", "not present", actual[i]))

        return

    # Handle dicts (nested messages)
    if isinstance(expected, dict) and isinstance(actual, dict):
        all_keys = set(expected.keys()) | set(actual.keys())

        for key in sorted(all_keys):
            exp_val = expected.get(key)
            act_val = actual.get(key)

            new_path = f"{path}.{key}" if path else key
            _compare_values(exp_val, act_val, new_path, differences)

        return

    # Handle floats with tolerance
    if isinstance(expected, float) and isinstance(actual, float):
        if not math.isclose(expected, actual, rel_tol=1e-9, abs_tol=1e-9):
            differences.append((path, expected, actual))
        return

    # Handle primitives (int, str, bool)
    if expected != actual:
        differences.append((path, expected, actual))


def compare_get_traces_responses(
    expected: GetTracesResponse,
    actual: GetTracesResponse,
) -> Optional[str]:
    """
    Compare two GetTracesResponse messages field by field.

    This utility recursively compares all fields in the responses and returns a
    human-readable summary of any differences found. It's designed to provide
    better debugging information than simple dict equality checks.

    Args:
        expected: The expected response
        actual: The actual response from the endpoint

    Returns:
        None if responses are equal, otherwise a formatted string showing all differences
        in the format:
            Differences found in GetTracesResponse:

              field.path:
                Expected: <value>
                Actual:   <value>

    Example:
        diff = compare_get_traces_responses(expected_response, response)
        assert diff is None, diff
    """
    # Convert to dicts for comparison
    expected_dict = MessageToDict(expected)
    actual_dict = MessageToDict(actual)

    # Track all differences
    differences: List[Tuple[str, Any, Any]] = []
    _compare_values(expected_dict, actual_dict, "", differences)

    if not differences:
        return None

    # Format the output
    lines = ["Differences found in GetTracesResponse:", ""]

    for path, exp_val, act_val in differences:
        # Use repr() for proper quoting of strings
        exp_repr = repr(exp_val) if isinstance(exp_val, str) else str(exp_val)
        act_repr = repr(act_val) if isinstance(act_val, str) else str(act_val)

        lines.append(f"  {path}:")
        lines.append(f"    Expected: {exp_repr}")
        lines.append(f"    Actual:   {act_repr}")
        lines.append("")

    return "\n".join(lines)
