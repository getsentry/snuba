import random
import uuid
from datetime import datetime, timedelta
from typing import Optional

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, TraceItem

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.helpers import write_raw_unprocessed_events

RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"
SERVER_NAME = "D23CXQ4GK2.local"

_DEFAULT_ATTRIBUTES = {
    "category": AnyValue(string_value="http"),
    "color": AnyValue(string_value=random.choice(["red", "green", "blue"])),
    "description": AnyValue(string_value="/api/0/relays/projectconfigs/"),
    "eap.measurement": AnyValue(int_value=random.choice([1, 100, 1000])),
    "environment": AnyValue(string_value="development"),
    "http.status_code": AnyValue(string_value="200"),
    "location": AnyValue(string_value=random.choice(["mobile", "frontend", "backend"])),
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
    "sentry.segment_id": AnyValue(string_value="8873a98879faf06d"),
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


def write_eap_span(
    start_timestamp: datetime,
    raw_attributes: dict[str, str | float | int | bool] | None = None,
    server_sample_rate: float = 1.0,
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
            )
            for _ in range(count)
        ],
    )


def gen_item_message(
    start_timestamp: datetime,
    attributes: dict[str, AnyValue] = {},
    type: TraceItemType.ValueType = TraceItemType.TRACE_ITEM_TYPE_SPAN,
    server_sample_rate: float = 1.0,
    client_sample_rate: float = 1.0,
    end_timestamp: Optional[datetime] = None,
) -> bytes:
    item_timestamp = Timestamp()
    item_timestamp.FromDatetime(start_timestamp)
    received = Timestamp()
    received.GetCurrentTime()
    if end_timestamp is None:
        end_timestamp = start_timestamp + timedelta(seconds=1)
    attributes.update(
        {
            "sentry.end_timestamp_precise": AnyValue(
                double_value=end_timestamp.timestamp()
            ),
            "sentry.received": AnyValue(double_value=received.seconds),
            "sentry.start_timestamp_precise": AnyValue(
                double_value=start_timestamp.timestamp()
            ),
            "start_timestamp_ms": AnyValue(
                double_value=(int(start_timestamp.timestamp() * 1000))
            ),
        }
    )
    return TraceItem(
        organization_id=1,
        project_id=1,
        item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        timestamp=item_timestamp,
        trace_id=uuid.uuid4().hex,
        item_id=uuid.uuid4().int.to_bytes(16, byteorder="little"),
        received=received,
        retention_days=90,
        server_sample_rate=server_sample_rate,
        client_sample_rate=client_sample_rate,
        attributes=_DEFAULT_ATTRIBUTES | attributes,
    ).SerializeToString()
