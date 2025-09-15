import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Optional

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ComparisonFilter,
    OrFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, TraceItem

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
}


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

    attributes: dict[str, AnyValue] = {}
    for key, value in raw_attributes.items():
        if isinstance(value, str):
            attributes[key] = AnyValue(string_value=value)
        elif isinstance(value, int):
            attributes[key] = AnyValue(int_value=value)
        elif isinstance(value, bool):
            attributes[key] = AnyValue(bool_value=value)
        else:
            attributes[key] = AnyValue(double_value=value)

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
        organization_id=1,
        project_id=1,
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
