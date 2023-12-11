import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple
from uuid import UUID

import pytest
from sentry_kafka_schemas.schema_types.snuba_spans_v1 import (
    SpanEvent,
    _MeasurementValue,
)
from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.spans_processor import SpansMessageProcessor
from snuba.processor import InsertBatch


@dataclass
class SpanEventExample:
    project_id: int
    duration_ms: int
    environment: Optional[str]
    event_id: str
    exclusive_time_ms: int
    group: str
    group_raw: str
    http_method: Optional[str]
    http_referer: Optional[str]
    module: str
    op: str
    organization_id: int
    parent_span_id: str
    platform: str
    received: float
    release: str
    retention_days: int
    segment_id: str
    span_id: str
    start_timestamp_ms: int
    status: str
    trace_id: str
    transaction_name: str
    user_id: Optional[str]
    user_name: Optional[str]
    measurements: Dict[str, _MeasurementValue]
    _metrics_summary: Dict[str, Any]

    def serialize(self) -> SpanEvent:
        return {
            "description": "SELECT `sentry_tagkey`.* FROM `sentry_tagkey`",
            "duration_ms": self.duration_ms,
            "exclusive_time_ms": self.exclusive_time_ms,
            "group_raw": self.group_raw,
            "is_segment": False,
            "parent_span_id": self.parent_span_id,
            "project_id": self.project_id,
            "received": self.received,
            "retention_days": self.retention_days,
            "segment_id": self.segment_id,
            "sentry_tags": {
                "http.method": "GET",
                "action": "SELECT",
                "domain": "targetdomain.tld:targetport",
                "module": self.module,
                "group": self.group,
                "status": "ok",
                "system": "python",
                "status_code": "200",
                "transaction": self.transaction_name,
                "transaction.op": self.op,
                "op": "http.client",
                "transaction.method": "GET",
            },
            "span_id": self.span_id,
            "start_timestamp_ms": self.start_timestamp_ms,
            "tags": {
                "tag1": "value1",
                "tag2": "123",
                "tag3": "True",
                "sentry:user": self.user_id or "",
            },
            "trace_id": self.trace_id,
            "measurements": self.measurements,
            "_metrics_summary": self._metrics_summary,
        }

    def build_result(self, meta: KafkaMessageMetadata) -> Sequence[Mapping[str, Any]]:
        return [
            {
                "project_id": self.project_id,
                "transaction_op": self.op,
                "trace_id": str(UUID(self.trace_id)),
                "span_id": int(self.span_id, 16),
                "parent_span_id": int(self.parent_span_id, 16),
                "segment_id": int(self.span_id, 16),
                "is_segment": 0,
                "segment_name": self.transaction_name,
                "start_timestamp": int(
                    datetime.fromtimestamp(
                        self.start_timestamp_ms / 1000,
                        tz=timezone.utc,
                    ).timestamp()
                ),
                "start_ms": self.start_timestamp_ms % 1000,
                "end_timestamp": int(
                    datetime.fromtimestamp(
                        (self.start_timestamp_ms + self.duration_ms) / 1000,
                        tz=timezone.utc,
                    ).timestamp()
                ),
                "end_ms": (self.start_timestamp_ms + self.duration_ms) % 1000,
                "duration": self.duration_ms,
                "exclusive_time": self.exclusive_time_ms,
                "op": "http.client",
                "group": int(self.group, 16),
                "group_raw": int(self.group_raw, 16),
                "span_status": SPAN_STATUS_NAME_TO_CODE.get("ok"),
                "span_kind": "",
                "description": "SELECT `sentry_tagkey`.* FROM `sentry_tagkey`",
                "status": SPAN_STATUS_NAME_TO_CODE.get("ok"),
                "module": self.module,
                "domain": "targetdomain.tld:targetport",
                "platform": self.platform,
                "action": "SELECT",
                "tags.key": [
                    "sentry:user",
                    "tag1",
                    "tag2",
                    "tag3",
                    "http.method",
                    "status_code",
                    "transaction.method",
                ],
                "tags.value": ["123", "value1", "123", "True", "GET", "200", "GET"],
                "measurements.key": ["memory"],
                "measurements.value": [1000.0],
                "partition": meta.partition,
                "offset": meta.offset,
                "retention_days": self.retention_days,
                "deleted": 0,
                "user": self.user_id,
                "sentry_tags.key": [
                    "action",
                    "domain",
                    "group",
                    "http.method",
                    "module",
                    "op",
                    "status",
                    "status_code",
                    "system",
                    "transaction",
                    "transaction.method",
                    "transaction.op",
                ],
                "sentry_tags.value": [
                    "SELECT",
                    "targetdomain.tld:targetport",
                    self.group,
                    "GET",
                    self.module,
                    "http.client",
                    "ok",
                    "200",
                    "python",
                    self.transaction_name,
                    "GET",
                    self.op,
                ],
                "metrics_summary": json.dumps(self._metrics_summary),
            },
        ]

    def build_metrics_summary_result(self) -> Sequence[Mapping[str, Any]]:
        common_fields = {
            "project_id": 1,
            "trace_id": str(UUID(self.trace_id)),
            "span_id": int(self.span_id, 16),
            "end_timestamp": int(
                datetime.fromtimestamp(
                    (self.start_timestamp_ms + self.duration_ms) / 1000,
                    tz=timezone.utc,
                ).timestamp()
            ),
            "deleted": 0,
            "retention_days": 90,
        }
        return [
            {
                **common_fields,
                **{
                    "metric_mri": "c:sentry.events.outcomes@none",
                    "count": 1,
                    "max": 1.0,
                    "min": 1.0,
                    "sum": 1.0,
                    "tags.key": [
                        "category",
                        "environment",
                        "event_type",
                        "outcome",
                        "release",
                        "topic",
                        "transaction",
                    ],
                    "tags.value": [
                        "error",
                        "unknown",
                        "error",
                        "accepted",
                        "backend@2af74c237fbd61489a1ccc46650f4f85befaf8b8",
                        "outcomes-billing",
                        "sentry.tasks.store.save_event",
                    ],
                },
            },
            {
                **common_fields,
                **{
                    "metric_mri": "c:sentry.events.post_save.normalize.errors@none",
                    "count": 1,
                    "max": 0.0,
                    "min": 0.0,
                    "sum": 0.0,
                    "tags.key": [
                        "environment",
                        "event_type",
                        "from_relay",
                        "release",
                        "transaction",
                    ],
                    "tags.value": [
                        "unknown",
                        "error",
                        "False",
                        "backend@2af74c237fbd61489a1ccc46650f4f85befaf8b8",
                        "sentry.tasks.store.save_event",
                    ],
                },
            },
        ]


def compare_types_and_values(dict1: Any, dict2: Any) -> bool:
    """
    Helper function to compare nested dicts. It is used to validate the results of span
    processing in the test cases with the expected results.
    """
    if isinstance(dict1, dict) and isinstance(dict2, dict):
        if len(dict1) != len(dict2):
            return False
        for key in dict1:
            if key not in dict2:
                raise KeyError(f"Key {key} not found in dict2")
            if not compare_types_and_values(dict1[key], dict2[key]):
                return False
        return True
    else:
        # Compare keys and values
        if dict1 == dict2:
            return True
        else:
            raise ValueError(f"Value {dict1} != {dict2}")


def __get_timestamps() -> Tuple[float, float, float]:
    timestamp = datetime.now(tz=timezone.utc) - timedelta(seconds=1000)
    start_timestamp = timestamp - timedelta(seconds=10)
    received = timestamp + timedelta(seconds=1)
    return received.timestamp(), start_timestamp.timestamp(), timestamp.timestamp()


def get_span_event() -> SpanEventExample:
    received, start, finish = __get_timestamps()
    return SpanEventExample(
        project_id=1,
        duration_ms=int(1000 * (finish - start)),
        environment="prod",
        event_id="e5e062bf2e1d4afd96fd2f90b6770431",
        exclusive_time_ms=int(1000 * (finish - start)),
        group="deadbeefdeadbeef",
        group_raw="deadbeefdeadbeef",
        http_method="POST",
        http_referer="tagstore.something",
        measurements={"memory": {"value": 1000.0}},
        module="http",
        op="navigation",
        organization_id=69,
        parent_span_id="deadbeefdeadbeef",
        platform="python",
        received=received,
        release="34a554c14b68285d8a8eb6c5c4c56dfc1db9a83a",
        retention_days=90,
        segment_id="deadbeefdeadbeef",
        span_id="deadbeefdeadbeef",
        start_timestamp_ms=int(start * 1000),
        status="cancelled",
        trace_id="deadbeefdeadbeefdeadbeefdeadbeef",
        transaction_name="/organizations/:orgId/issues/",
        user_id="123",
        user_name="me",
        _metrics_summary={
            "c:sentry.events.outcomes@none": [
                {
                    "count": 1,
                    "max": 1.0,
                    "min": 1.0,
                    "sum": 1.0,
                    "tags": {
                        "category": "error",
                        "environment": "unknown",
                        "event_type": "error",
                        "outcome": "accepted",
                        "release": "backend@2af74c237fbd61489a1ccc46650f4f85befaf8b8",
                        "topic": "outcomes-billing",
                        "transaction": "sentry.tasks.store.save_event",
                    },
                }
            ],
            "c:sentry.events.post_save.normalize.errors@none": [
                {
                    "count": 1,
                    "max": 0.0,
                    "min": 0.0,
                    "sum": 0.0,
                    "tags": {
                        "environment": "unknown",
                        "event_type": "error",
                        "from_relay": "False",
                        "release": "backend@2af74c237fbd61489a1ccc46650f4f85befaf8b8",
                        "transaction": "sentry.tasks.store.save_event",
                    },
                }
            ],
        },
    )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestSpansProcessor:
    def test_required_clickhouse_columns_are_present(self) -> None:
        message = get_span_event()
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        actual_result = SpansMessageProcessor().process_message(
            message.serialize(), meta
        )
        assert isinstance(actual_result, InsertBatch)
        rows = actual_result.rows
        expected_result = message.build_result(meta)
        assert len(rows) == len(expected_result)

        for index in range(len(rows)):
            assert set(rows[index]) - set(expected_result[index]) == set()
            assert set(expected_result[index]) - set(rows[index]) == set()

        for index in range(len(rows)):
            assert len(rows[index]) == len(expected_result[index])

    def test_exact_results(self) -> None:
        message = get_span_event()
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        actual_result = SpansMessageProcessor().process_message(
            message.serialize(), meta
        )

        assert isinstance(actual_result, InsertBatch)
        rows = actual_result.rows

        expected_result = message.build_result(meta)
        assert len(rows) == len(expected_result)
        for index in range(len(rows)):
            assert compare_types_and_values(rows[index], expected_result[index])
