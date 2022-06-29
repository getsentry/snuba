import json
from datetime import datetime, timedelta
from typing import Any, Callable, Tuple, Union

import pytest
import pytz

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.metrics_messages import InputType

# from snuba.datasets.entities import EntityKey
# from snuba.datasets.entities.factory import get_entity
from snuba.datasets.storages.generic_metrics import sets_bucket_storage
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages

RETENTION_DAYS = 90
SNQL_ROUTE = "/generic_metrics/snql"


def utc_yesterday_12_15() -> datetime:
    return (datetime.utcnow() - timedelta(days=1)).replace(
        hour=12, minute=15, second=0, microsecond=0, tzinfo=pytz.utc
    )


class TestGenericMetricsApiSets(BaseApiTest):
    placeholder_counter = 0

    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "metrics_counters"

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[[str], Any]) -> None:
        self.post = _build_snql_post_methods

    def placeholder(self) -> str:
        self.placeholder_counter += 1
        return "placeholder{:04d}".format(self.placeholder_counter)

    def setup_method(self, test_method: Any) -> None:
        super().setup_method(test_method)

        self.write_storage = sets_bucket_storage
        self.count = 10
        self.org_id = 1
        self.project_id = 2
        self.metric_id = 3
        self.base_time = utc_yesterday_12_15()
        self.default_tags = {
            "65546": 65536,
            "9223372036854776010": 65593,
            "9223372036854776016": 109333,
            "9223372036854776020": 65616,
            "9223372036854776021": 9223372036854776027,
            "9223372036854776022": 65539,
            "9223372036854776023": 65555,
            "9223372036854776026": 9223372036854776031,
        }
        self.mapping_meta = {
            "c": {
                "65536": self.placeholder(),
                "65539": self.placeholder(),
                "65546": self.placeholder(),
                "65555": self.placeholder(),
                "65593": self.placeholder(),
                "65616": self.placeholder(),
                "109333": self.placeholder(),
            },
            "h": {
                "9223372036854775908": self.placeholder(),
                "9223372036854776010": self.placeholder(),
                "9223372036854776016": self.placeholder(),
                "9223372036854776020": self.placeholder(),
                "9223372036854776021": self.placeholder(),
                "9223372036854776022": self.placeholder(),
                "9223372036854776023": self.placeholder(),
                "9223372036854776026": self.placeholder(),
                "9223372036854776027": self.placeholder(),
                "9223372036854776031": self.placeholder(),
            },
        }
        self.set_values = [50, 100, 150]
        self.use_case_id = "performance"
        self.start_time = self.base_time
        self.end_time = (
            self.base_time + timedelta(seconds=self.count) + timedelta(seconds=10)
        )
        self.generate_sets()

    def generate_sets(self) -> None:
        rows = [
            self.write_storage.get_table_writer()
            .get_stream_loader()
            .get_processor()
            .process_message(
                {
                    "org_id": self.org_id,
                    "project_id": self.project_id,
                    "unit": "ms",
                    "type": InputType.SET.value,
                    "value": self.set_values,
                    "timestamp": self.base_time.timestamp() + n,
                    "tags": self.default_tags,
                    "metric_id": self.metric_id,
                    "retention_days": RETENTION_DAYS,
                    "mapping_meta": self.mapping_meta,
                    "use_case_id": self.use_case_id,
                },
                KafkaMessageMetadata(0, 0, self.base_time),
            )
            for n in range(self.count)
        ]
        write_processed_messages(self.write_storage, [row for row in rows if row])

    def test_retrieval_basic(self) -> None:
        query_str = f"""MATCH (generic_metrics_sets)
                    SELECT uniq(value) AS unique_values BY project_id, org_id
                    WHERE org_id = {self.org_id}
                    AND project_id = {self.project_id}
                    AND metric_id = {self.metric_id}
                    AND timestamp >= toDateTime('{self.start_time}')
                    AND timestamp < toDateTime('{self.end_time}')
                    GRANULARITY 1
                    """
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps({"query": query_str, "dataset": "generic_metrics"}),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 1, data
