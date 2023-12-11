from datetime import datetime

import pytest

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.metrics_summaries_processor import (
    MetricsSummariesMessageProcessor,
)
from snuba.processor import InsertBatch
from tests.datasets.test_spans_processor import compare_types_and_values, get_span_event


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestMetricsSummaryProcessor:
    def test_required_clickhouse_columns_are_present(self) -> None:
        message = get_span_event()
        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        actual_result = MetricsSummariesMessageProcessor().process_message(
            message.serialize(), meta
        )
        assert isinstance(actual_result, InsertBatch)
        rows = actual_result.rows
        expected_result = message.build_metrics_summary_result()
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
        actual_result = MetricsSummariesMessageProcessor().process_message(
            message.serialize(), meta
        )

        assert isinstance(actual_result, InsertBatch)
        rows = actual_result.rows

        expected_result = message.build_metrics_summary_result()
        assert len(rows) == len(expected_result)
        for index in range(len(rows)):
            assert compare_types_and_values(rows[index], expected_result[index])
