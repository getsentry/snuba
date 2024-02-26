import json
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone

import pytest
from sentry_kafka_schemas.schema_types.snuba_metrics_summaries_v1 import MetricsSummary

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.base import BaseApiTest
from tests.datasets.processors.test_metrics_summaries_processor import (
    build_metrics_summary_payload,
)
from tests.helpers import write_processed_messages
from tests.test_metrics_summaries_api import utc_yesterday_12_15

SNQL_ROUTE = "/storages/metrics_summaries/snql"


@pytest.mark.clickhouse_db
def test_basic_query(
    self,
    project_id: int,
    metric_mri: str,
    start_time: datetime,
    end_time: datetime,
    writable_table_storage: WritableTableStorage,
    unique_span_ids: Sequence[str],
) -> None:
    # not rly sure what this line does, im guessing it populates local data
    # generate_metrics_summaries(writable_table_storage)
    # I kept this query the same bc all the field names match the storage
    query_str = f"""MATCH (metrics_summaries)
                    SELECT groupUniqArray(span_id) AS unique_span_ids BY project_id, metric_mri
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
                "storage": "metric_summaries",
                "tenant_ids": {"referrer": "tests", "organization_id": 1},
            }
        ),
    )
    data = json.loads(response.data)

    # kept the same asserts bc we probably want the same output invarients to hold
    assert response.status_code == 200
    assert len(data["data"]) == 1, data
    assert data["data"][0]["unique_span_ids"] == unique_span_ids
