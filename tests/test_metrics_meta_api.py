import json
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Generator, Mapping, Tuple, Union

import pytest

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.metrics_messages import InputType
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages

RETENTION_DAYS = 90
SNQL_ROUTE = "/generic_metrics/snql"


def get_tags() -> Generator[Mapping[str, str], None, None]:
    idx = 0
    mappings = {"environment": "112358", "release": "132134"}
    while True:
        environment = ["prod", "dev", "staging", "test"][idx % 4]
        release = f"1.0.{idx}"
        yield {
            mappings["environment"]: environment,
            mappings["release"]: release,
        }
        idx += 1


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGenericMetricsApiSets(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "generic_metrics_sets"

    @pytest.fixture
    def test_storage_set(self) -> StorageKey:
        return StorageKey.GENERIC_METRICS_SETS_RAW

    @pytest.fixture
    def test_metric_type(self) -> InputType:
        return InputType.SET

    def generate_metric_values(self) -> Any:
        return [1, 2, 3]

    @pytest.fixture(autouse=True)
    def setup_teardown(
        self,
        test_storage_set: StorageKey,
        test_metric_type: InputType,
        clickhouse_db: None,
        _build_snql_post_methods: Callable[[str], Any],
    ) -> None:
        self.post = _build_snql_post_methods

        self.write_storage = get_storage(test_storage_set)
        self.count = 10
        self.org_id, self.project_id = 10, 20
        self.metric_ids = [4, 9, 16]
        self.sentry_received_timestamp = datetime.now(UTC) - timedelta(minutes=2)
        self.tag_generator = get_tags()
        self.use_case_id = "performance"
        self.start_time = datetime.now(UTC) - timedelta(days=90)
        self.end_time = datetime.now(UTC) + timedelta(days=0)
        self.generate_metrics(
            test_metric_type,
        )

    def generate_metrics(
        self,
        metric_type: InputType,
    ) -> None:
        assert isinstance(self.write_storage, WritableTableStorage)
        for metric_id in self.metric_ids:
            rows = [
                self.write_storage.get_table_writer()
                .get_stream_loader()
                .get_processor()
                .process_message(
                    {
                        "org_id": self.org_id,
                        "project_id": self.project_id,
                        "unit": "ms",
                        "type": metric_type.value,
                        "value": self.generate_metric_values(),
                        "timestamp": self.sentry_received_timestamp.timestamp() + n,
                        "tags": next(self.tag_generator),
                        "metric_id": metric_id,
                        "retention_days": RETENTION_DAYS,
                        "use_case_id": self.use_case_id,
                        "sentry_received_timestamp": self.sentry_received_timestamp.timestamp() + n,
                    },
                    KafkaMessageMetadata(0, 0, self.sentry_received_timestamp),
                )
                for n in range(self.count)
            ]
            write_processed_messages(self.write_storage, [row for row in rows if row])

    def test_retrieve_metric_names(self, test_entity: str) -> None:
        entity_name = f"{test_entity}_meta"
        query_str = f"""MATCH ({entity_name})
                    SELECT metric_id
                    BY metric_id
                    WHERE org_id = {self.org_id}
                    AND project_id = {self.project_id}
                    AND use_case_id = '{self.use_case_id}'
                    AND timestamp >= toDateTime('{self.start_time.isoformat()}')
                    AND timestamp < toDateTime('{self.end_time.isoformat()}')
                    ORDER BY metric_id ASC
                    """
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(
                {
                    "query": query_str,
                    "dataset": "generic_metrics",
                    "tenant_ids": {"referrer": "tests", "organization_id": 1},
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 3, data
        assert data["data"][0]["metric_id"] == 4
        assert data["data"][1]["metric_id"] == 9
        assert data["data"][2]["metric_id"] == 16

    def test_retrieve_tag_keys(self, test_entity: str) -> None:
        entity_name = f"{test_entity}_meta"
        query_str = f"""MATCH ({entity_name})
                    SELECT tag_key
                    BY tag_key
                    WHERE org_id = {self.org_id}
                    AND project_id = {self.project_id}
                    AND use_case_id = '{self.use_case_id}'
                    AND metric_id = {self.metric_ids[0]}
                    AND timestamp >= toDateTime('{self.start_time.isoformat()}')
                    AND timestamp < toDateTime('{self.end_time.isoformat()}')
                    ORDER BY tag_key ASC
                    """
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(
                {
                    "query": query_str,
                    "dataset": "generic_metrics",
                    "tenant_ids": {"referrer": "tests", "organization_id": 1},
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 2, data
        assert data["data"][0]["tag_key"] == 112358
        assert data["data"][1]["tag_key"] == 132134


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGenericMetricsApiCounters(TestGenericMetricsApiSets):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "generic_metrics_counters"

    @pytest.fixture
    def test_storage_set(self) -> StorageKey:
        return StorageKey.GENERIC_METRICS_COUNTERS_RAW

    @pytest.fixture
    def test_metric_type(self) -> InputType:
        return InputType.COUNTER

    def generate_metric_values(self) -> Any:
        return 1.0


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGenericMetricsApiGauges(TestGenericMetricsApiSets):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "generic_metrics_gauges"

    @pytest.fixture
    def test_storage_set(self) -> StorageKey:
        return StorageKey.GENERIC_METRICS_GAUGES_RAW

    @pytest.fixture
    def test_metric_type(self) -> InputType:
        return InputType.GAUGE

    def generate_metric_values(self) -> Any:
        return {
            "min": 2.0,
            "max": 21.0,
            "sum": 25.0,
            "count": 3,
            "last": 4.0,
        }


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGenericMetricsApiDistributions(TestGenericMetricsApiSets):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "generic_metrics_distributions"

    @pytest.fixture
    def test_storage_set(self) -> StorageKey:
        return StorageKey.GENERIC_METRICS_DISTRIBUTIONS_RAW

    @pytest.fixture
    def test_metric_type(self) -> InputType:
        return InputType.DISTRIBUTION

    def generate_metric_values(self) -> Any:
        return [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
