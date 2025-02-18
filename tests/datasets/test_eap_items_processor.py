from datetime import UTC, datetime

import pytest

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.eap_items_processor import EAPItemsProcessor
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import InsertBatch
from tests.datasets.test_spans_processor import get_span_event
from tests.helpers import write_processed_messages
from tests.web.rpc.v1.test_endpoint_trace_item_table.test_endpoint_trace_item_table import (
    gen_message,
)

topic_span = {
    "description": "SELECT `sentry_tagkey`.* FROM `sentry_tagkey`",
    "duration_ms": 10000,
    "exclusive_time_ms": 10000,
    "end_timestamp_precise": 1739575973.482,
    "is_segment": False,
    "parent_span_id": "deadbeefdeadbeef",
    "project_id": 1,
    "organization_id": 1,
    "received": 1739575974.48267,
    "retention_days": 90,
    "segment_id": "deadbeefdeadbeef",
    "sentry_tags": {
        "http.method": "GET",
        "action": "SELECT",
        "domain": "targetdomain.tld:targetport",
        "module": "http",
        "group": "deadbeefdeadbeef",
        "status": "ok",
        "system": "python",
        "status_code": "200",
        "transaction": "/organizations/:orgId/issues/",
        "transaction.op": "navigation",
        "op": "http.client",
        "transaction.method": "GET",
    },
    "span_id": "deadbeefdeadbeef",
    "start_timestamp_ms": 1739575963482,
    "start_timestamp_precise": 1739575963.482,
    "tags": {"tag1": "value1", "tag2": "123", "tag3": "True"},
    "trace_id": "deadbeefdeadbeefdeadbeefdeadbeef",
    "measurements": {"memory": {"value": 1000.0}},
    "event_id": "e5e062bf2e1d4afd96fd2f90b6770431",
}


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestEAPItemsProcessor:
    def test_can_write_to_storage(self) -> None:
        now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        topic_span = gen_message(now)
        meta = KafkaMessageMetadata(offset=1, partition=2, timestamp=now)
        result = EAPItemsProcessor().process_message(topic_span, meta)
        assert isinstance(result, InsertBatch)
        write_processed_messages(
            get_writable_storage(StorageKey("eap_items")),
            [result],
        )

    def test_exact_results(self) -> None:
        message = get_span_event()
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        actual_result = EAPItemsProcessor().process_message(message.serialize(), meta)

        assert isinstance(actual_result, InsertBatch)
        rows = actual_result.rows

        expected_result = message.build_result(meta)
        assert len(rows) == len(expected_result)
        for index in range(len(rows)):
            assert rows[index] == expected_result[index]
