import random
import uuid
from datetime import UTC, datetime

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, TraceItem

_RELEASE_TAG = "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b"
_SERVER_NAME = "D23CXQ4GK2.local"

_DEFAULT_ATTRIBUTES = {
    "category": AnyValue(string_value="http"),
    "color": AnyValue(string_value=random.choice(["red", "green", "blue"])),
    "description": AnyValue(string_value="/api/0/relays/projectconfigs/"),
    "sentry.duration_ms": AnyValue(int_value=152),
    "eap.measurement": AnyValue(int_value=random.choice([1, 100, 1000])),
    "environment": AnyValue(string_value="development"),
    "sentry.event_id": AnyValue(string_value="d826225de75d42d6b2f01b957d51f18f"),
    "sentry.exclusive_time_ms": AnyValue(double_value=0.228),
    "http.status_code": AnyValue(string_value="200"),
    "sentry.is_segment": AnyValue(bool_value=True),
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
    "release": AnyValue(string_value=_RELEASE_TAG),
    "sdk.name": AnyValue(string_value="sentry.python.django"),
    "sdk.version": AnyValue(string_value="2.7.0"),
    "sentry.segment_id": AnyValue(string_value="8873a98879faf06d"),
    "sentry.environment": AnyValue(string_value="development"),
    "sentry.release": AnyValue(string_value=_RELEASE_TAG),
    "sentry.sdk.name": AnyValue(string_value="sentry.python.django"),
    "sentry.sdk.version": AnyValue(string_value="2.7.0"),
    "sentry.segment.name": AnyValue(string_value="/api/0/relays/projectconfigs/"),
    "server_name": AnyValue(string_value=_SERVER_NAME),
    "spans_over_limit": AnyValue(string_value="False"),
    "status": AnyValue(string_value="ok"),
    "status_code": AnyValue(string_value="200"),
    "thread.id": AnyValue(string_value="8522009600"),
    "thread.name": AnyValue(string_value="uWSGIWorker1Core0"),
    "trace.status": AnyValue(string_value="ok"),
    "transaction": AnyValue(string_value="/api/0/relays/projectconfigs/"),
    "sentry.raw_description": AnyValue(string_value="/api/0/relays/projectconfigs/"),
    "transaction.method": AnyValue(string_value="POST"),
    "transaction.op": AnyValue(string_value="http.server"),
    "user": AnyValue(string_value="ip:127.0.0.1"),
}


def gen_item_message(
    dt: datetime,
    attributes: dict[str, AnyValue] = {},
    type: TraceItemType.ValueType = TraceItemType.TRACE_ITEM_TYPE_SPAN,
) -> bytes:
    item_timestamp = Timestamp()
    item_timestamp.FromDatetime(dt)
    received = Timestamp()
    received.GetCurrentTime()
    attributes.update(
        {
            "sentry.received": AnyValue(double_value=received.seconds),
            "sentry.end_timestamp_precise": AnyValue(
                double_value=dt.replace(tzinfo=UTC).timestamp() + 1
            ),
            "start_timestamp_ms": AnyValue(
                double_value=(
                    int(dt.replace(tzinfo=UTC).timestamp()) * 1000
                    - int(random.gauss(1000, 200))
                )
            ),
            "sentry.start_timestamp_precise": AnyValue(
                double_value=dt.replace(tzinfo=UTC).timestamp()
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
        attributes=_DEFAULT_ATTRIBUTES | attributes,
    ).SerializeToString()
