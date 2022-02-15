from datetime import datetime, timedelta
from typing import Any, Callable, Tuple, Union

import pytest
import pytz
import simplejson as json

from snuba import state
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.metrics_aggregate_processor import METRICS_COUNTERS_TYPE
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages

SNQL_ROUTE = "/metrics/snql"
LIMIT_BY_COUNT = 5
TAG_1_KEY = "6"
TAG_1_VALUE_1 = 91
TAG_2_KEY = "9"
TAG_2_VALUE_1 = 134
TAG_3_KEY = "4"
TAG_3_VALUE_1 = 159
TAG_4_KEY = "5"
TAG_4_VALUE_1 = 34
RETENTION_DAYS = 90


class TestMetricsApiCounters(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "metrics_counters"

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[[str], Any]) -> None:
        self.post = _build_snql_post_methods

    def setup_method(self, test_method: Any) -> None:
        super().setup_method(test_method)

        # values for test data
        self.metric_id = 1001
        self.org_id = 101
        self.project_ids = [1, 2]  # 2 projects
        self.seconds = 180 * 60

        self.default_tags = {
            TAG_1_KEY: TAG_1_VALUE_1,
            TAG_2_KEY: TAG_2_VALUE_1,
            TAG_3_KEY: TAG_3_VALUE_1,
            TAG_4_KEY: TAG_4_VALUE_1,
        }
        self.skew = timedelta(seconds=self.seconds)

        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        ) - timedelta(minutes=self.seconds)
        self.storage = get_writable_storage(StorageKey.METRICS_COUNTERS_BUCKETS)
        self.generate_counters()

    def teardown_method(self, test_method: Any) -> None:
        # Reset rate limits
        state.delete_config("global_concurrent_limit")
        state.delete_config("global_per_second_limit")
        state.delete_config("project_concurrent_limit")
        state.delete_config("project_concurrent_limit_1")
        state.delete_config("project_per_second_limit")
        state.delete_config("date_align_seconds")

    def generate_counters(self) -> None:
        """
        Generate some counts

        distribution sample message:
        {
            "org_id": 1,
            "project_id": 2,
            "name": "sentry.transactions.transaction.duration",
            "unit": "ms",
            "type": "d",
            "value": [
                21,
                76,
                116,
                142,
                179
            ],
            "timestamp": 1641418510,
            "tags": {
                "6": 91,
                "9": 134,
                "4": 159,
                "5": 34
            },
            "metric_id": 8,
            "retention_days": 90
        }
        {"org_id":1,"project_id":2,"name":"sentry.transactions.transaction.duration","unit":"ms","type":"d","value":[21.0,76.0,116.0,142.0,179.0],"timestamp":1641418510,"tags":{"6":91,"9":134,"4":159,"5":34},"metric_id":8,"retention_days":90}
        {"org_id":1,"project_id":2,"name":"sentry.transactions.transaction.duration","unit":"ms","type":"d","value":[24.0,80.0,119.0,146.0,182.0],"timestamp":1641418510,"tags":{"6":91,"9":134,"4":117,"5":7},"metric_id":8,"retention_days":90}
        """
        events = []
        for n in range(self.seconds):
            for p in self.project_ids:
                # this counter increases metric
                processed = (
                    self.storage.get_table_writer()
                    .get_stream_loader()
                    .get_processor()
                    .process_message(
                        (
                            {
                                "org_id": self.org_id,
                                "project_id": p,
                                "unit": "ms",
                                "type": METRICS_COUNTERS_TYPE,
                                "value": 1.0,
                                "timestamp": self.base_time.timestamp() + n,
                                "tags": self.default_tags,
                                "metric_id": self.metric_id,
                                "retention_days": RETENTION_DAYS,
                            }
                        ),
                        KafkaMessageMetadata(0, 0, self.base_time),
                    )
                )
                if processed:
                    events.append(processed)
        write_processed_messages(self.storage, events)

    def test_retrieval_basic(self) -> None:
        query_str = """MATCH (metrics_counters)
                    SELECT sumMerge(value) AS total_seconds BY project_id, org_id
                    WHERE org_id = {org_id}
                    AND project_id = 1
                    AND metric_id = {metric_id}
                    AND granularity = 60
                    AND timestamp >= toDateTime('{start_time}')
                    AND timestamp < toDateTime('{end_time}')
                    LIMIT 1
                    """.format(
            metric_id=self.metric_id,
            org_id=self.org_id,
            start_time=(self.base_time - self.skew).isoformat(),
            end_time=(self.base_time + self.skew).isoformat(),
        )
        response = self.app.post(
            SNQL_ROUTE, data=json.dumps({"query": query_str, "dataset": "transactions"})
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 1, data

        aggregation = data["data"][0]
        assert aggregation["org_id"] == self.org_id
        assert aggregation["project_id"] == self.project_ids[0]
        assert aggregation["total_seconds"] == self.seconds
