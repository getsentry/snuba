import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence, Tuple

import pytest

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.spans_processor import (
    SpansMessageProcessor,
    is_project_in_allowlist,
)
from snuba.processor import InsertBatch
from snuba.state import delete_config, set_config


@dataclass
class TransactionEvent:
    event_id: str
    trace_id: str
    span_id: str
    transaction_name: str
    op: str
    start_timestamp: float
    timestamp: float
    platform: str
    dist: Optional[str]
    user_name: Optional[str]
    user_id: Optional[str]
    environment: Optional[str]
    release: str
    http_method: Optional[str]
    http_referer: Optional[str]
    status: str

    def __post_init__(self):
        self.span1_start_timestamp = (
            datetime.utcfromtimestamp(self.start_timestamp) + timedelta(seconds=1)
        ).timestamp()
        self.span1_end_timestamp = (
            datetime.utcfromtimestamp(self.timestamp) + timedelta(seconds=3)
        ).timestamp()
        self.span2_start_timestamp = self.span1_start_timestamp + 1000
        self.span2_end_timestamp = self.span1_end_timestamp

    def serialize(self) -> Tuple[int, str, Mapping[str, Any]]:
        return (
            2,
            "insert",
            {
                "datetime": "2019-08-08T22:29:53.917000Z",
                "organization_id": 1,
                "platform": self.platform,
                "project_id": 1,
                "event_id": self.event_id,
                "message": "/organizations/:orgId/issues/",
                "retention_days": 30,
                "data": {
                    "event_id": self.event_id,
                    "environment": self.environment,
                    "project_id": 1,
                    "release": self.release,
                    "dist": self.dist,
                    "transaction_info": {"source": "url"},
                    "grouping_config": {
                        "enhancements": "eJybzDhxY05qemJypZWRgaGlroGxrqHRBABbEwcC",
                        "id": "legacy:2019-03-12",
                    },
                    "sdk": {
                        "packages": [{"version": "0.9.0", "name": "pypi:sentry-sdk"}],
                    },
                    "breadcrumbs": {
                        "values": [
                            {
                                "category": "query",
                                "timestamp": 1565308204.544,
                                "message": "[Filtered]",
                                "type": "default",
                                "level": "info",
                            },
                        ],
                    },
                    "contexts": {
                        "trace": {
                            "sampled": True,
                            "trace_id": self.trace_id,
                            "op": self.op,
                            "type": "trace",
                            "span_id": self.span_id,
                            "status": self.status,
                            "hash": "a" * 16,
                            "exclusive_time": 1.2345,
                        },
                    },
                    "spans": [
                        {
                            "sampled": True,
                            "start_timestamp": self.span1_start_timestamp,
                            "same_process_as_parent": None,
                            "description": "GET /api/0/organizations/sentry/tags/?project=1",
                            "tags": None,
                            "timestamp": self.span1_end_timestamp,
                            "parent_span_id": self.span_id,
                            "trace_id": self.trace_id,
                            "span_id": str(int(self.span_id, 16) + 1),
                            "data": {},
                            "op": "http.client",
                            "hash": "b" * 16,
                            "exclusive_time": 0.1234,
                        },
                        {
                            "sampled": True,
                            "start_timestamp": self.span2_start_timestamp,
                            "same_process_as_parent": None,
                            "description": "SELECT `sentry_tagkey`.* FROM `sentry_tagkey`",
                            "tags": None,
                            "timestamp": self.span2_end_timestamp,
                            "parent_span_id": str(int(self.span_id, 16) + 1),
                            "trace_id": self.trace_id,
                            "span_id": str(int(self.span_id, 16) + 2),
                            "data": {},
                            "op": "db",
                            "hash": "c" * 16,
                            "exclusive_time": 0.4567,
                        },
                    ],
                    "platform": self.platform,
                    "version": "7",
                    "location": "/organizations/:orgId/issues/",
                    "logger": "",
                    "type": "transaction",
                    "metadata": {
                        "location": "/organizations/:orgId/issues/",
                        "title": "/organizations/:orgId/issues/",
                    },
                    "primary_hash": "d41d8cd98f00b204e9800998ecf8427e",
                    "datetime": "2019-08-08T22:29:53.917000Z",
                    "timestamp": self.timestamp,
                    "start_timestamp": self.start_timestamp,
                    "measurements": {
                        "lcp": {"value": 32.129},
                        "lcp.elementSize": {"value": 4242},
                        "fid": {"value": None},
                        "invalid": None,
                        "invalid2": {},
                    },
                    "breakdowns": {
                        "span_ops": {
                            "ops.db": {"value": 62.512},
                            "ops.http": {"value": 109.774},
                            "total.time": {"value": 172.286},
                        }
                    },
                    "tags": [
                        ["sentry:release", self.release],
                        ["sentry:user", self.user_id],
                        ["environment", self.environment],
                        ["random_key", "random_value"],
                    ],
                    "user": {
                        "username": self.user_name,
                    },
                    "request": {
                        "url": "http://127.0.0.1:/query",
                        "headers": [
                            ["Accept-Encoding", "identity"],
                            ["Content-Length", "398"],
                            ["Host", "127.0.0.1:"],
                            ["Referer", self.http_referer],
                            ["Trace", "8fa73032d-1"],
                        ],
                        "data": "",
                        "method": self.http_method,
                        "env": {"SERVER_PORT": "1010", "SERVER_NAME": "snuba"},
                    },
                    "transaction": self.transaction_name,
                },
            },
        )

    def build_result(self, meta: KafkaMessageMetadata) -> Sequence[Mapping[str, Any]]:
        start_timestamp = datetime.utcfromtimestamp(self.start_timestamp)
        finish_timestamp = datetime.utcfromtimestamp(self.timestamp)

        ret = [
            {
                "project_id": 1,
                "transaction_id": str(uuid.UUID(self.event_id)),
                "transaction_op": self.op,
                "trace_id": str(uuid.UUID(self.trace_id)),
                "span_id": int(self.span_id, 16),
                "parent_span_id": 0,
                "segment_id": int(self.span_id, 16),
                "is_segment": 1,
                "segment_name": self.transaction_name,
                "start_timestamp": start_timestamp,
                "end_timestamp": finish_timestamp,
                "duration": int(
                    (finish_timestamp - start_timestamp).total_seconds() * 1000
                ),
                "exclusive_time": 1.2345,
                "op": self.op,
                "group": "a" * 16,
                "span_status": self.status,
                "span_kind": "SERVER",
                "description": "/organizations/:orgId/issues/",
                "status": 200,
                "module": "api",
                "action": "GET",
                "domain": "127.0.0.1",
                "platform": "",
                "user": self.user_id,
                "tags.key": ["environment", "release", "user", "random_key"],
                "tags.value": [
                    self.environment,
                    self.release,
                    self.user_id,
                    "random_value",
                ],
                "measurements.key": ["lcp", "lcp.elementSize"],
                "measurements.value": [32.129, 4242],
                "partition": meta.partition,
                "offset": meta.offset,
                "retention_days": 30,
                "deleted": 0,
            },
            {
                "project_id": 1,
                "transaction_id": str(uuid.UUID(self.event_id)),
                "transaction_op": self.op,
                "trace_id": str(uuid.UUID(self.trace_id)),
                "span_id": int("b70840cd33074881", 16),
                "parent_span_id": 0,
                "segment_id": int(self.span_id, 16),
                "is_segment": 0,
                "segment_name": self.transaction_name,
                "start_timestamp": start_timestamp,
                "end_timestamp": finish_timestamp,
                "duration": int(
                    (finish_timestamp - start_timestamp).total_seconds() * 1000
                ),
                "exclusive_time": 1.2345,
                "op": "http.client",
                "group": "b" * 16,
                "span_status": self.status,
                "span_kind": "span",
                "description": "GET /api/0/organizations/sentry/tags/?project=1",
                "status": 200,
                "module": "sentry",
                "domain": "",
                "platform": "",
                "tags.key": ["environment", "release", "user", "random_key"],
                "tags.value": [
                    self.environment,
                    self.release,
                    self.user_id,
                    "random_value",
                ],
                "measurements.key": [],
                "measurements.value": [],
                "partition": meta.partition,
                "offset": meta.offset,
                "retention_days": 30,
                "deleted": 0,
            },
            {
                "project_id": 1,
                "transaction_id": str(uuid.UUID(self.event_id)),
                "transaction_op": self.op,
                "trace_id": str(uuid.UUID(self.trace_id)),
                "span_id": int(self.span_id, 16),
                "parent_span_id": 0,
                "segment_id": int(self.span_id, 16),
                "is_segment": 1,
                "segment_name": self.transaction_name,
                "start_timestamp": start_timestamp,
                "end_timestamp": finish_timestamp,
                "duration": int(
                    (finish_timestamp - start_timestamp).total_seconds() * 1000
                ),
                "exclusive_time": 1.2345,
                "op": "db",
                "group": "c" * 16,
                "span_status": self.status,
                "span_kind": "span",
                "description": "SELECT `sentry_tagkey`.* FROM `sentry_tagkey`",
                "status": 200,
                "module": "sentry",
                "domain": "http",
                "platform": self.platform,
                "tags.key": ["environment", "release", "user", "random_key"],
                "tags.value": [
                    self.environment,
                    self.release,
                    self.user_id,
                    "random_value",
                ],
                "measurements.key": [],
                "measurements.value": [],
                "partition": meta.partition,
                "offset": meta.offset,
                "retention_days": 30,
                "deleted": 0,
            },
        ]

        return ret


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestSpansProcessor:
    @staticmethod
    def __get_timestamps() -> Tuple[float, float]:
        timestamp = datetime.now(tz=timezone.utc) - timedelta(seconds=5)
        start_timestamp = timestamp - timedelta(seconds=10)
        return start_timestamp.timestamp(), timestamp.timestamp()

    def __get_transaction_event(self) -> TransactionEvent:
        start, finish = self.__get_timestamps()
        return TransactionEvent(
            event_id="e5e062bf2e1d4afd96fd2f90b6770431",
            trace_id="7400045b25c443b885914600aa83ad04",
            span_id="8841662216cc598b",
            transaction_name="/organizations/:orgId/issues/",
            status="cancelled",
            op="navigation",
            timestamp=finish,
            start_timestamp=start,
            platform="python",
            dist="",
            user_name="me",
            user_id="123",
            environment="prod",
            release="34a554c14b68285d8a8eb6c5c4c56dfc1db9a83a",
            http_method="POST",
            http_referer="tagstore.something",
        )

    def test_required_clickhouse_columns_are_present(self) -> None:
        set_config("spans_project_allowlist", "[1]")
        message = self.__get_transaction_event()

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


@pytest.mark.parametrize(
    "sample_rate, project_id, input_project_id, expected_result",
    [
        (0, 100, 100, True),
        (0, 101, 100, False),
        (1, 101, 100, True),
        (1, 101, 101, True),
        (1, 101, 102, False),
        (None, 100, 100, True),
        (None, 101, 100, False),
    ],
)
@pytest.mark.redis_db
def test_is_project_in_allowlist(
    sample_rate, project_id, input_project_id, expected_result
):
    if sample_rate:
        set_config("spans_sample_rate", sample_rate)
    if project_id:
        set_config("spans_project_allowlist", f"[{project_id}]")

    assert is_project_in_allowlist(input_project_id) == expected_result

    delete_config("spans_sample_rate")
    delete_config("spans_project_allowlist")
