import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, NamedTuple, Sequence, Tuple

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.spans_processor import SpansMessageProcessor
from snuba.processor import InsertBatch, json_encode_insert_batch


class SpanData(NamedTuple):
    trace_id: str
    span_id: str
    parent_span_id: str
    op: str
    start_timestamp: float
    timestamp: float

    def serialize(self) -> Mapping[str, Any]:
        return {
            "sampled": True,
            "start_timestamp": self.start_timestamp,
            "same_process_as_parent": None,
            "description": "GET /api/0/organizations/sentry/tags/?project=1",
            "timestamp": self.timestamp,
            "parent_span_id": self.parent_span_id,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "op": self.op,
            "tags": [["some_tag", "some_val"]],
        }


@dataclass
class SpanEvent:
    event_id: str
    trace_id: str
    span_id: str
    parent_span_id: str
    transaction_name: str
    op: str
    start_timestamp: float
    timestamp: float
    spans: Sequence[SpanData]

    def serialize(self) -> Tuple[int, str, Mapping[str, Any]]:
        return (
            2,
            "insert",
            {
                "datetime": "2019-08-08T22:29:53.917000Z",
                "organization_id": 1,
                "platform": "python",
                "project_id": 1,
                "event_id": self.event_id,
                "message": "/organizations/:orgId/issues/",
                "group_id": None,
                "data": {
                    "event_id": self.event_id,
                    "environment": "prod",
                    "project_id": 1,
                    "release": "34a554c14b68285d8a8eb6c5c4c56dfc1db9a83a",
                    "dist": "",
                    "sdk": {
                        "version": "0.9.0",
                        "name": "sentry-sdk",
                        "packages": [{"version": "0.9.0", "name": "pypi:sentry-sdk"}],
                    },
                    "spans": [s.serialize() for s in self.spans],
                    "platform": "python",
                    "version": "7",
                    "type": "transaction",
                    "retention_days": None,
                    "datetime": "2019-08-08T22:29:53.917000Z",
                    "timestamp": self.timestamp,
                    "start_timestamp": self.start_timestamp,
                    "contexts": {
                        "trace": {
                            "sampled": True,
                            "trace_id": self.trace_id,
                            "op": self.op,
                            "type": "trace",
                            "span_id": self.span_id,
                            "status": "ok",
                            "parent_span_id": self.parent_span_id,
                        },
                    },
                    "tags": [
                        ["sentry:release", "34a554c14b68285d8a8eb6c5c4c56dfc1db9a83a"],
                    ],
                    "user": {"username": "me"},
                    "transaction": self.transaction_name,
                },
            },
        )

    def build_result(self, meta: KafkaMessageMetadata) -> Sequence[Mapping[str, Any]]:
        start_timestamp = datetime.utcfromtimestamp(self.start_timestamp)
        finish_timestamp = datetime.utcfromtimestamp(self.timestamp)
        ret = [
            {
                "deleted": 0,
                "project_id": 1,
                "transaction_id": str(uuid.UUID(self.event_id)),
                "retention_days": 90,
                "transaction_span_id": int(self.span_id, 16),
                "trace_id": str(uuid.UUID(self.trace_id)),
                "transaction_name": self.transaction_name,
                "span_id": int(self.span_id, 16),
                "parent_span_id": int(self.parent_span_id, 16),
                "op": self.op,
                "status": 0,
                "start_ts": start_timestamp,
                "start_ns": int(start_timestamp.microsecond * 1000),
                "finish_ts": finish_timestamp,
                "finish_ns": int(finish_timestamp.microsecond * 1000),
                "duration_ms": int(
                    (finish_timestamp - start_timestamp).total_seconds() * 1000
                ),
            }
        ]

        for s in self.spans:
            span_start_ts = datetime.utcfromtimestamp(s.start_timestamp)
            span_finish_ts = datetime.utcfromtimestamp(s.timestamp)
            ret.append(
                {
                    "deleted": 0,
                    "project_id": 1,
                    "transaction_id": str(uuid.UUID(self.event_id)),
                    "retention_days": 90,
                    "transaction_span_id": int(self.span_id, 16),
                    "trace_id": str(uuid.UUID(self.trace_id)),
                    "transaction_name": self.transaction_name,
                    "span_id": int(s.span_id, 16),
                    "parent_span_id": int(s.parent_span_id, 16),
                    "op": s.op,
                    "tags.key": ["some_tag"],
                    "tags.value": ["some_val"],
                    "status": 2,
                    "start_ts": span_start_ts,
                    "start_ns": int(span_start_ts.microsecond * 1000),
                    "finish_ts": span_finish_ts,
                    "finish_ns": int(span_finish_ts.microsecond * 1000),
                    "duration_ms": int(
                        (span_finish_ts - span_start_ts).total_seconds() * 1000
                    ),
                }
            )
        return ret


def test_span_process() -> None:
    timestamp = datetime.now(tz=timezone.utc) - timedelta(seconds=5)
    start_timestamp = timestamp - timedelta(seconds=4)
    message = SpanEvent(
        event_id="e5e062bf2e1d4afd96fd2f90b6770431",
        trace_id="7400045b25c443b885914600aa83ad04",
        span_id="8841662216cc598b",
        parent_span_id="b76a8ca0b0908a15",
        transaction_name="/organizations/:orgId/issues/",
        op="navigation",
        timestamp=timestamp.timestamp(),
        start_timestamp=start_timestamp.timestamp(),
        spans=[
            SpanData(
                trace_id="7400045b25c443b885914600aa83ad04",
                span_id="b95eff64930fef25",
                parent_span_id="8841662216cc598b",
                op="db",
                start_timestamp=(start_timestamp + timedelta(seconds=1)).timestamp(),
                timestamp=(start_timestamp + timedelta(seconds=2)).timestamp(),
            ),
            SpanData(
                trace_id="7400045b25c443b885914600aa83ad04",
                span_id="9f8e7bbe7bf22e09",
                parent_span_id="b95eff64930fef25",
                op="web",
                start_timestamp=(start_timestamp + timedelta(seconds=2)).timestamp(),
                timestamp=(start_timestamp + timedelta(seconds=3)).timestamp(),
            ),
        ],
    )
    meta = KafkaMessageMetadata(offset=1, partition=2, timestamp=datetime(1970, 1, 1))
    processed = SpansMessageProcessor().process_message(message.serialize(), meta)
    expected_rows = message.build_result(meta)

    assert processed == json_encode_insert_batch(InsertBatch(expected_rows, None))
