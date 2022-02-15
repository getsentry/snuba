from datetime import datetime, timedelta
from typing import Any, Callable, Tuple, Union

import pytest
import pytz
import simplejson as json
from pytest import approx

from snuba import state
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.metrics_aggregate_processor import (
    METRICS_COUNTERS_TYPE,
    METRICS_DISTRIBUTIONS_TYPE,
    METRICS_SET_TYPE,
)
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
        events = []
        for n in range(self.seconds):
            for p in self.project_ids:
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
                    """.format(
            metric_id=self.metric_id,
            org_id=self.org_id,
            start_time=(self.base_time - self.skew).isoformat(),
            end_time=(self.base_time + self.skew).isoformat(),
        )
        response = self.app.post(
            SNQL_ROUTE, data=json.dumps({"query": query_str, "dataset": "metrics"})
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 1, data

        aggregation = data["data"][0]
        assert aggregation["org_id"] == self.org_id
        assert aggregation["project_id"] == self.project_ids[0]
        assert aggregation["total_seconds"] == self.seconds


class TestMetricsApiSets(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "metrics_sets"

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[[str], Any]) -> None:
        self.post = _build_snql_post_methods

    def setup_method(self, test_method: Any) -> None:
        super().setup_method(test_method)

        # values for test data
        self.metric_id = 1002
        self.org_id = 103
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
        self.storage = get_writable_storage(StorageKey.METRICS_BUCKETS)
        self.unique_set_values = 100
        self.generate_sets()

    def teardown_method(self, test_method: Any) -> None:
        # Reset rate limits
        state.delete_config("global_concurrent_limit")
        state.delete_config("global_per_second_limit")
        state.delete_config("project_concurrent_limit")
        state.delete_config("project_concurrent_limit_1")
        state.delete_config("project_per_second_limit")
        state.delete_config("date_align_seconds")

    def generate_sets(self) -> None:
        events = []
        processor = self.storage.get_table_writer().get_stream_loader().get_processor()

        for n in range(self.seconds):
            for p in self.project_ids:
                msg = {
                    "org_id": self.org_id,
                    "project_id": p,
                    "type": METRICS_SET_TYPE,
                    "value": [n % self.unique_set_values],
                    "timestamp": self.base_time.timestamp() + n,
                    "tags": self.default_tags,
                    "metric_id": self.metric_id,
                    "retention_days": RETENTION_DAYS,
                }

                processed = processor.process_message(
                    msg, KafkaMessageMetadata(0, 0, self.base_time),
                )
                if processed:
                    events.append(processed)
        write_processed_messages(self.storage, events)

    def test_sets_basic(self) -> None:
        query_str = """MATCH (metrics_sets)
                    SELECT uniqCombined64Merge(value) AS unique_values BY project_id, org_id
                    WHERE org_id = {org_id}
                    AND project_id = 1
                    AND metric_id = {metric_id}
                    AND granularity = 60
                    AND timestamp >= toDateTime('{start_time}')
                    AND timestamp < toDateTime('{end_time}')
                    """.format(
            metric_id=self.metric_id,
            org_id=self.org_id,
            start_time=(self.base_time - self.skew).isoformat(),
            end_time=(self.base_time + self.skew).isoformat(),
        )
        response = self.app.post(
            SNQL_ROUTE, data=json.dumps({"query": query_str, "dataset": "metrics"})
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 1, data

        aggregation = data["data"][0]

        assert aggregation["org_id"] == self.org_id
        assert aggregation["project_id"] == self.project_ids[0]
        assert aggregation["unique_values"] == self.unique_set_values


class TestMetricsApiDistributions(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "metrics_distributions"

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[[str], Any]) -> None:
        self.post = _build_snql_post_methods

    def setup_method(self, test_method: Any) -> None:
        super().setup_method(test_method)

        # values for test data
        self.metric_id = 1010
        self.org_id = 103
        self.project_ids = [1, 2]  # 2 projects
        self.seconds = 180 * 60
        self.d_range_min, self.d_range_max = (0, 100)

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
        self.storage = get_writable_storage(StorageKey.METRICS_DISTRIBUTIONS_BUCKETS)
        self.generate_uniform_distributions()

    def teardown_method(self, test_method: Any) -> None:
        # Reset rate limits
        state.delete_config("global_concurrent_limit")
        state.delete_config("global_per_second_limit")
        state.delete_config("project_concurrent_limit")
        state.delete_config("project_concurrent_limit_1")
        state.delete_config("project_per_second_limit")
        state.delete_config("date_align_seconds")

    def generate_uniform_distributions(self) -> None:
        events = []
        processor = self.storage.get_table_writer().get_stream_loader().get_processor()
        value_array = list(range(self.d_range_min, self.d_range_max))

        for n in range(self.seconds):
            for p in self.project_ids:
                msg = {
                    "org_id": self.org_id,
                    "project_id": p,
                    "type": METRICS_DISTRIBUTIONS_TYPE,
                    "value": value_array,
                    "timestamp": self.base_time.timestamp() + n,
                    "tags": self.default_tags,
                    "metric_id": self.metric_id,
                    "retention_days": RETENTION_DAYS,
                }

                processed = processor.process_message(
                    msg, KafkaMessageMetadata(0, 0, self.base_time),
                )
                if processed:
                    events.append(processed)
        write_processed_messages(self.storage, events)

    def test_dists_basic(self) -> None:
        query_str = """MATCH (metrics_distributions)
                    SELECT quantilesMerge(0.5,0.9,0.95,0.99)(percentiles) AS quants BY project_id, org_id
                    WHERE org_id = {org_id}
                    AND project_id = 1
                    AND metric_id = {metric_id}
                    AND granularity = 60
                    AND timestamp >= toDateTime('{start_time}')
                    AND timestamp < toDateTime('{end_time}')
                    """.format(
            metric_id=self.metric_id,
            org_id=self.org_id,
            start_time=(self.base_time - self.skew).isoformat(),
            end_time=(self.base_time + self.skew).isoformat(),
        )
        response = self.app.post(
            SNQL_ROUTE, data=json.dumps({"query": query_str, "dataset": "metrics"})
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 1, data

        aggregation = data["data"][0]

        assert aggregation["org_id"] == self.org_id
        assert aggregation["project_id"] == self.project_ids[0]
        assert aggregation["quants"] == [
            approx(50, rel=1),
            approx(90),
            approx(95),
            approx(99),
        ]
