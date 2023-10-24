# It's hard to programmatically code up payloads for spans, so we just
# have a few examples here.

import datetime
import uuid
from typing import Any, Dict, Generator, Mapping, Sequence

import pytest
from sentry_kafka_schemas.schema_types.snuba_spans_v1 import SpanEvent
from structlog.testing import capture_logs

from snuba import state
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.spans_processor import SpansMessageProcessor
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import InsertBatch
from tests.datasets.test_spans_processor import compare_types_and_values
from tests.helpers import write_processed_messages

# some time in way in the future
start_time_ms = 4111111111111
duration_ms = 1234560123
project_id = 1234567890123456

required_fields = {
    "trace_id": "12345678901234567890123456789012",
    "span_id": "1234567890123456",
    "project_id": project_id,
    "start_timestamp_ms": start_time_ms,
    "duration_ms": duration_ms,
    "exclusive_time_ms": 1234567890123,
    "is_segment": True,
    "retention_days": 90,
    "sentry_tags": {},
}

expected_for_required_fields = {
    "deleted": 0,
    "retention_days": 90,
    "partition": 2,
    "offset": 1,
    "project_id": project_id,
    "trace_id": "12345678-9012-3456-7890-123456789012",
    "span_id": 1311768467284833366,
    "segment_id": 1311768467284833366,
    "is_segment": True,
    "description": "",
    "group_raw": 0,
    "start_timestamp": int(
        datetime.datetime.fromtimestamp(start_time_ms / 1000.0).timestamp()
    ),
    "start_ms": 111,
    "end_timestamp": int(
        datetime.datetime.fromtimestamp(
            (start_time_ms + duration_ms) / 1000.0
        ).timestamp()
    ),
    "end_ms": 234,
    "duration": 1234560123,
    "exclusive_time": 1234567890123,
    "tags.key": [],
    "tags.value": [],
    "user": "",
    "measurements.key": [],
    "measurements.value": [],
    "sentry_tags.key": [],
    "sentry_tags.value": [],
    "module": "",
    "action": "",
    "domain": "",
    "group": 0,
    "span_kind": "",
    "platform": "",
    "segment_name": "",
    "span_status": 2,
    "op": "",
}

payloads = [
    {**required_fields},
    {**required_fields, **{"description": "test span"}},
    {
        **required_fields,
        **{
            "event_id": "12345678901234567890123456789012",
            "profile_id": "deadbeefdeadbeefdeadbeefdeadbeef",
        },
    },
    {**required_fields, **{"group_raw": "deadbeefdeadbeef"}},
    {**required_fields, **{"tags": {"tag1": "value1", "tag2": 123, "tag3": True}}},
    {
        **required_fields,
        **{
            "tags": {
                "sentry:user": "user1",
            }
        },
    },
    {
        **required_fields,
        **{
            "sentry_tags": {
                "status": "ok",
                "status_code": 200,
            }
        },
    },
    {
        **required_fields,
        **{
            "sentry_tags": {
                "status": "ok",
                "status_code": 200,
                "group": "deadbeefdeadbeef",
            }
        },
    },
    {
        **required_fields,
        **{
            "sentry_tags": {
                "transaction.method": "GET",
                "user": "user1",
                "release": "release1234",
            }
        },
    },
]

expected_results: Sequence[Mapping[str, Any]] = [
    {**expected_for_required_fields},
    {**expected_for_required_fields, **{"description": "test span"}},
    {
        **expected_for_required_fields,
        **{
            "transaction_id": str(uuid.UUID("12345678901234567890123456789012")),
            "profile_id": str(uuid.UUID("deadbeefdeadbeefdeadbeefdeadbeef")),
        },
    },
    {**expected_for_required_fields, **{"group_raw": int("deadbeefdeadbeef", 16)}},
    {
        **expected_for_required_fields,
        **{
            "tags.key": ["tag1", "tag2", "tag3"],
            "tags.value": ["value1", "123", "True"],
        },
    },
    {
        **expected_for_required_fields,
        **{"user": "user1", "tags.key": ["sentry:user"], "tags.value": ["user1"]},
    },
    {
        **expected_for_required_fields,
        **{
            "span_status": 0,
            "status": 0,
            "tags.key": ["status_code"],
            "tags.value": ["200"],
        },
        **{
            "sentry_tags.key": ["status", "status_code"],
            "sentry_tags.value": ["ok", "200"],
        },
    },
    {
        **expected_for_required_fields,
        **{
            "span_status": 0,
            "status": 0,
            "group": int("deadbeefdeadbeef", 16),
            "tags.key": ["status_code"],
            "tags.value": ["200"],
        },
        **{
            "sentry_tags.key": ["group", "status", "status_code"],
            "sentry_tags.value": ["deadbeefdeadbeef", "ok", "200"],
        },
    },
    {
        **expected_for_required_fields,
        **{
            "tags.key": ["release", "transaction.method", "user"],
            "tags.value": ["release1234", "GET", "user1"],
        },
        **{
            "sentry_tags.key": ["release", "transaction.method", "user"],
            "sentry_tags.value": ["release1234", "GET", "user1"],
        },
    },
]

assert len(payloads) == len(expected_results)


@pytest.mark.redis_db
class TestSpansPayloads:
    @pytest.fixture()
    def setup_state(self, redis_db: None) -> Generator[None, None, None]:
        state.set_config("log_bad_span_message_percentage", 1)
        yield
        state.delete_config("log_bad_span_message_percentage")

    @pytest.mark.parametrize(
        "payload, expected_result", zip(payloads, expected_results)
    )
    def test_spans_payloads(
        self, setup_state: None, payload: SpanEvent, expected_result: Dict[str, Any]
    ) -> None:
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime.datetime(1970, 1, 1)
        )
        assert payload
        processed_rows = SpansMessageProcessor().process_message(payload, meta)

        assert isinstance(processed_rows, InsertBatch), processed_rows
        actual_result = processed_rows.rows[0]
        assert compare_types_and_values(actual_result, expected_result)

    def test_fail_missing_required(self, setup_state: None, caplog: Any) -> None:
        payload1: Any = required_fields.copy()
        del payload1["trace_id"]
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime.datetime(1970, 1, 1)
        )
        processed = SpansMessageProcessor().process_message(payload1, meta)
        assert processed is None

        payload2: Any = required_fields.copy()
        del payload2["span_id"]
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime.datetime(1970, 1, 1)
        )
        with capture_logs() as logs:
            processed = SpansMessageProcessor().process_message(payload2, meta)
            assert processed is None
            assert "Failed to process span message" in str(logs)


class TestWriteSpansClickhouse:
    @pytest.mark.clickhouse_db
    def test_write_spans_to_clickhouse(self) -> None:
        storage = get_writable_storage(StorageKey.SPANS)
        batch = InsertBatch(rows=expected_results, origin_timestamp=None)
        write_processed_messages(storage, [batch])
