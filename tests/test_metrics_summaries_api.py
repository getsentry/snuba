import json
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

import pytest

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.base import BaseApiTest
from tests.datasets.test_spans_processor import get_span_event
from tests.helpers import write_processed_messages

RETENTION_DAYS = 90
SNQL_ROUTE = "/spans/snql"


def utc_yesterday_12_15() -> datetime:
    return (datetime.utcnow() - timedelta(days=1)).replace(
        hour=12, minute=15, second=0, microsecond=0, tzinfo=timezone.utc
    )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestMetricsSummariesApi(BaseApiTest):
    @pytest.fixture
    def writable_table_storage(self) -> Any:
        return get_storage(StorageKey.METRICS_SUMMARIES)

    @pytest.fixture
    def project_id(self) -> int:
        return 1

    @pytest.fixture
    def metric_mri(self) -> str:
        return "c:sentry.events.outcomes@none"

    @pytest.fixture
    def unique_span_ids(self) -> Sequence[str]:
        return ["deadbeefdeadbeef"]

    @pytest.fixture
    def start_time(self) -> datetime:
        return utc_yesterday_12_15()

    @pytest.fixture
    def end_time(self) -> datetime:
        return utc_yesterday_12_15() + timedelta(days=30)

    @pytest.fixture
    def span_event(self) -> Mapping[str, Any]:
        return get_span_event().serialize()

    def generate_metrics_summaries(
        self,
        span_event: bytes,
        writable_table_storage: WritableTableStorage,
    ) -> None:
        assert isinstance(writable_table_storage, WritableTableStorage)
        rows = [
            writable_table_storage.get_table_writer()
            .get_stream_loader()
            .get_processor()
            .process_message(
                span_event,
                KafkaMessageMetadata(0, 0, utc_yesterday_12_15()),
            )
        ]
        write_processed_messages(writable_table_storage, [row for row in rows if row])

    def test_basic_query(
        self,
        project_id: int,
        metric_mri: str,
        start_time: datetime,
        end_time: datetime,
        span_event: bytes,
        writable_table_storage: WritableTableStorage,
        unique_span_ids: Sequence[str],
    ) -> None:
        self.generate_metrics_summaries(span_event, writable_table_storage)

        query_str = f"""MATCH (metrics_summaries)
                    SELECT groupUniqArray(span_id) AS unique_span_ids BY project_id
                    WHERE project_id = {project_id}
                    AND metric_mri = '{metric_mri}'
                    AND end_timestamp >= toDateTime('{start_time}')
                    AND end_timestamp < toDateTime('{end_time}')
                    GRANULARITY 60
                    """
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(
                {
                    "query": query_str,
                    "dataset": "spans",
                    "tenant_ids": {"referrer": "tests", "organization_id": 1},
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 1, data
        assert data["data"][0]["unique_span_ids"] == unique_span_ids

    def test_tags_query(
        self,
        project_id: int,
        metric_mri: str,
        start_time: datetime,
        end_time: datetime,
        span_event: bytes,
        writable_table_storage: WritableTableStorage,
        unique_span_ids: Sequence[str],
    ) -> None:
        self.generate_metrics_summaries(span_event, writable_table_storage)

        tag_key = "topic"
        tag_value = "outcomes-billing"

        query_str = f"""MATCH (metrics_summaries)
                    SELECT groupUniqArray(span_id) AS unique_span_ids BY project_id
                    WHERE project_id = {project_id}
                    AND metric_mri = '{metric_mri}'
                    AND tags[{tag_key}] = '{tag_value}'
                    AND end_timestamp >= toDateTime('{start_time}')
                    AND end_timestamp < toDateTime('{end_time}')
                    GRANULARITY 60
                    """
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(
                {
                    "query": query_str,
                    "dataset": "spans",
                    "tenant_ids": {"referrer": "tests", "organization_id": 1},
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 1, data
        assert data["data"][0]["unique_span_ids"] == unique_span_ids
