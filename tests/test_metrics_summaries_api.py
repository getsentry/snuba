import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Tuple, Union

import pytest

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.base import BaseApiTest
from tests.datasets.test_spans_processor import get_span_event
from tests.helpers import write_processed_messages

RETENTION_DAYS = 90
SNQL_ROUTE = "/metrics_summaries/snql"


def utc_yesterday_12_15() -> datetime:
    return (datetime.utcnow() - timedelta(days=1)).replace(
        hour=12, minute=15, second=0, microsecond=0, tzinfo=timezone.utc
    )


@pytest.mark.clickhouse_db
class TestMetricsSummariesApi(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "metrics_summaries"

    @pytest.fixture(autouse=True)
    def setup_teardown(
        self, clickhouse_db: None, _build_snql_post_methods: Callable[[str], Any]
    ) -> None:
        self.post = _build_snql_post_methods

        self.write_storage = get_storage(StorageKey.METRICS_SUMMARIES)

        self.project_id = 1
        self.metric_mri = "c:sentry.events.outcomes@none"
        self.unique_span_ids = [int("deadbeafdeadbeef", 16)]

        self.base_time = utc_yesterday_12_15()
        self.start_time = self.base_time
        self.end_time = self.base_time + timedelta(seconds=10)

        self.generate_metrics_summaries()

    def generate_metrics_summaries(
        self,
    ) -> None:
        assert isinstance(self.write_storage, WritableTableStorage)
        rows = [
            self.write_storage.get_table_writer()
            .get_stream_loader()
            .get_processor()
            .process_message(
                get_span_event().serialize(),
                KafkaMessageMetadata(0, 0, self.base_time),
            )
        ]
        write_processed_messages(self.write_storage, [row for row in rows if row])

    def test_basic_query(self) -> None:
        query_str = f"""MATCH (metrics_summaries)
                    SELECT uniq(span_id) AS unique_span_ids BY project_id, metric_mri
                    WHERE project_id = {self.project_id}
                    AND metric_mri = {self.metric_mri}
                    AND count >= 1
                    AND timestamp >= toDateTime('{self.start_time}')
                    AND timestamp < toDateTime('{self.end_time}')
                    GRANULARITY 60
                    """
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(
                {
                    "query": query_str,
                    "dataset": "metrics_summaries",
                    "tenant_ids": {"referrer": "tests", "organization_id": 1},
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 1, data
        assert data["data"][0]["unique_span_ids"] == self.unique_span_ids

    def test_tags_query(self) -> None:
        tag_key = "topic"
        tag_value = "outcomes-billing"

        query_str = f"""MATCH (metrics_summaries)
                    SELECT uniq(span_id) AS unique_span_ids BY project_id, metric_mri
                    WHERE project_id = {self.project_id}
                    AND metric_mri = {self.metric_mri}
                    AND tags[{tag_key}] = {tag_value}
                    AND timestamp >= toDateTime('{self.start_time}')
                    AND timestamp < toDateTime('{self.end_time}')
                    GRANULARITY 60
                    """
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(
                {
                    "query": query_str,
                    "dataset": "metrics_summaries",
                    "tenant_ids": {"referrer": "tests", "organization_id": 1},
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 1, data
        assert data["data"][0]["unique_span_ids"] == self.unique_span_ids
