from datetime import datetime
from typing import Any, Mapping, Sequence

import pytest
from sentry_kafka_schemas import get_codec
from sentry_kafka_schemas.codecs import Codec
from sentry_kafka_schemas.schema_types.snuba_metrics_summaries_v1 import MetricsSummary

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.metrics_summaries_processor import (
    MetricsSummariesMessageProcessor,
)
from snuba.processor import InsertBatch
from tests.datasets.test_spans_processor import compare_types_and_values

METRICS_SUMMARY_SCHEMA: Codec[MetricsSummary] = get_codec("snuba-metrics-summaries")


def build_metrics_summary_payload() -> MetricsSummary:
    return MetricsSummary(
        {
            "count": 1,
            "duration_ms": 1000,
            "end_timestamp": 1707862430.123,
            "group": "deadbeefdeadbeef",
            "is_segment": False,
            "max": 1.0,
            "min": 1.0,
            "mri": "t:namespace/name@unit",
            "project_id": 42,
            "received": 1707862428.123,
            "retention_days": 90,
            "segment_id": "deadbeefdeadbeef",
            "span_id": "deadbeefdeadbeef",
            "sum": 1.0,
            "tags": {
                "tag1": "value1",
                "tag2": "123",
                "tag3": "True",
            },
            "trace_id": "1729c61b15a9450da6adea88aae8fa15",
        }
    )


def build_metrics_summary_result(
    summary: MetricsSummary,
) -> Sequence[Mapping[str, Any]]:
    return [
        {
            "count": summary.get("count", 0),
            "deleted": 0,
            "duration_ms": summary["duration_ms"],
            "end_timestamp": summary["end_timestamp"],
            "group": int(summary["group"], 16),
            "is_segment": summary["is_segment"],
            "max": summary.get("max", 0.0),
            "metric_mri": summary["mri"],
            "min": summary.get("min", 0.0),
            "project_id": summary["project_id"],
            "retention_days": summary["retention_days"],
            "segment_id": int(summary["segment_id"], 16),
            "span_id": int(summary["span_id"], 16),
            "sum": summary.get("sum", 0.0),
            "tags.key": list(summary.get("tags", {}).keys()),
            "tags.value": list(summary.get("tags", {}).values()),
            "trace_id": summary["trace_id"],
        }
    ]


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestMetricsSummaryProcessor:
    def test_required_clickhouse_columns_are_present(self) -> None:
        metrics_summary = build_metrics_summary_payload()
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        actual_result = MetricsSummariesMessageProcessor().process_message(
            metrics_summary, meta
        )
        assert isinstance(actual_result, InsertBatch)
        rows = actual_result.rows
        expected_result = build_metrics_summary_result(metrics_summary)
        assert len(rows) == len(expected_result)

        for index in range(len(rows)):
            assert set(rows[index]) - set(expected_result[index]) == set()
            assert set(expected_result[index]) - set(rows[index]) == set()

        for index in range(len(rows)):
            assert len(rows[index]) == len(expected_result[index])

    def test_exact_results(self) -> None:
        metrics_summary = build_metrics_summary_payload()
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        actual_result = MetricsSummariesMessageProcessor().process_message(
            metrics_summary, meta
        )

        assert isinstance(actual_result, InsertBatch)
        rows = actual_result.rows

        expected_result = build_metrics_summary_result(metrics_summary)
        assert len(rows) == len(expected_result)

        for index in range(len(rows)):
            assert compare_types_and_values(rows[index], expected_result[index])
