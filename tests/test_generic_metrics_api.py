import itertools
import json
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable, Mapping, Tuple, Union

import pytest
import pytz
from pytest import approx

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.metrics_messages import InputType
from snuba.datasets.storages.generic_metrics import (
    distributions_bucket_storage,
    sets_bucket_storage,
)
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages

RETENTION_DAYS = 90
SNQL_ROUTE = "/generic_metrics/snql"


def utc_yesterday_12_15() -> datetime:
    return (datetime.utcnow() - timedelta(days=1)).replace(
        hour=12, minute=15, second=0, microsecond=0, tzinfo=pytz.utc
    )


placeholder_counter = 0


def gen_string() -> str:
    global placeholder_counter
    placeholder_counter += 1
    return "placeholder{:04d}".format(placeholder_counter)


SHARED_TAGS: Mapping[str, int] = {
    "65546": 65536,
    "9223372036854776010": 65593,
    "9223372036854776016": 109333,
    "9223372036854776020": 65616,
    "9223372036854776021": 9223372036854776027,
    "9223372036854776022": 65539,
    "9223372036854776023": 65555,
    "9223372036854776026": 9223372036854776031,
}

SHARED_MAPPING_META: Mapping[str, Mapping[str, str]] = {
    "c": {
        "65536": gen_string(),
        "65539": gen_string(),
        "65546": gen_string(),
        "65555": gen_string(),
        "65593": gen_string(),
        "65616": gen_string(),
        "109333": gen_string(),
    },
    "h": {
        "9223372036854775908": gen_string(),
        "9223372036854776010": gen_string(),
        "9223372036854776016": gen_string(),
        "9223372036854776020": gen_string(),
        "9223372036854776021": gen_string(),
        "9223372036854776022": gen_string(),
        "9223372036854776023": gen_string(),
        "9223372036854776026": gen_string(),
        "9223372036854776027": gen_string(),
        "9223372036854776031": gen_string(),
    },
}


class TestGenericMetricsApiSets(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "generic_metrics_sets"

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[[str], Any]) -> None:
        self.post = _build_snql_post_methods

    def setup_method(self, test_method: Any) -> None:
        super().setup_method(test_method)

        self.write_storage = sets_bucket_storage
        self.count = 10
        self.org_id = 1
        self.project_id = 2
        self.metric_id = 3
        self.base_time = utc_yesterday_12_15()
        self.default_tags = SHARED_TAGS
        self.mapping_meta = SHARED_MAPPING_META
        self.unique_values = 5
        self.set_values = range(0, self.unique_values)
        self.set_cycle = itertools.cycle(self.set_values)
        self.use_case_id = "performance"
        self.start_time = self.base_time
        self.end_time = (
            self.base_time + timedelta(seconds=self.count) + timedelta(seconds=10)
        )
        self.generate_sets(self.default_tags, self.mapping_meta, self.set_cycle)

    def generate_sets(
        self,
        tags: Mapping[str, int],
        mapping_meta: Mapping[str, Mapping[str, str]],
        int_source: Iterable[int],
    ) -> None:
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
                    "value": list(itertools.islice(int_source, 3)),
                    "timestamp": self.base_time.timestamp() + n,
                    "tags": tags,
                    "metric_id": self.metric_id,
                    "retention_days": RETENTION_DAYS,
                    "mapping_meta": mapping_meta,
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
        assert data["data"][0]["unique_values"] == self.unique_values

    def test_raw_tags(self) -> None:
        tag_key = 1337
        tag_idx_value = 123456
        new_set_unique_count = 10
        new_tag_values = {str(tag_key): tag_idx_value}
        value_as_string = gen_string()
        new_mapping_meta = {
            "d": {str(tag_key): gen_string(), str(tag_idx_value): value_as_string}
        }
        new_set_values = itertools.cycle(range(0, new_set_unique_count))

        self.generate_sets(
            tags=new_tag_values,
            mapping_meta=new_mapping_meta,
            int_source=new_set_values,
        )

        query_str = f"""MATCH (generic_metrics_sets)
                    SELECT uniq(value) AS unique_values BY project_id, org_id
                    WHERE org_id = {self.org_id}
                    AND project_id = {self.project_id}
                    AND metric_id = {self.metric_id}
                    AND tags_raw[{tag_key}] = '{value_as_string}'
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
        assert data["data"][0]["unique_values"] == new_set_unique_count

    def test_indexed_tags(self) -> None:
        tag_key = 1337
        tag_idx_value = 123456
        new_set_unique_count = 12
        new_tag_values = {str(tag_key): tag_idx_value}
        value_as_string = gen_string()
        new_mapping_meta = {
            "d": {str(tag_key): gen_string(), str(tag_idx_value): value_as_string}
        }
        new_set_values = itertools.cycle(range(0, new_set_unique_count))

        self.generate_sets(
            tags=new_tag_values,
            mapping_meta=new_mapping_meta,
            int_source=new_set_values,
        )

        query_str = f"""MATCH (generic_metrics_sets)
                    SELECT uniq(value) AS unique_values BY project_id, org_id
                    WHERE org_id = {self.org_id}
                    AND project_id = {self.project_id}
                    AND metric_id = {self.metric_id}
                    AND tags[{tag_key}] = {tag_idx_value}
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
        assert data["data"][0]["unique_values"] == new_set_unique_count


class TestGenericMetricsApiDistributions(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "generic_metrics_distributions"

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[[str], Any]) -> None:
        self.post = _build_snql_post_methods

    def setup_method(self, test_method: Any) -> None:
        super().setup_method(test_method)

        self.write_storage = distributions_bucket_storage
        self.count = 10
        self.org_id = 1
        self.project_id = 2
        self.metric_id = 3
        self.base_time = utc_yesterday_12_15()
        self.default_tags = SHARED_TAGS
        self.mapping_meta = SHARED_MAPPING_META

        #
        # This distribution will contain 0-4 repeating in equal
        # proportions
        #
        self.range_max = 5
        self.dist_values = range(0, self.range_max)
        self.raw_dist_value_cycle = itertools.cycle(self.dist_values)

        self.use_case_id = "performance"
        self.start_time = self.base_time
        self.end_time = (
            self.base_time + timedelta(seconds=self.count) + timedelta(seconds=10)
        )
        self.generate_dists()

    def generate_dists(self) -> None:
        rows = [
            self.write_storage.get_table_writer()
            .get_stream_loader()
            .get_processor()
            .process_message(
                {
                    "org_id": self.org_id,
                    "project_id": self.project_id,
                    "unit": "ms",
                    "type": InputType.DISTRIBUTION.value,
                    "value": list(itertools.islice(self.raw_dist_value_cycle, 10)),
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
        query_str = f"""MATCH (generic_metrics_distributions)
                    SELECT min(value) AS dist_min,
                           max(value) AS dist_max,
                           avg(value) AS dist_avg,
                           sum(value) AS dist_sum,
                           count(value) AS dist_count
                    BY project_id, org_id
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
        assert data["data"][0]["dist_min"] == 0.0
        assert data["data"][0]["dist_max"] == 4.0
        assert data["data"][0]["dist_avg"] == 2.0
        assert data["data"][0]["dist_sum"] == 200.0
        assert data["data"][0]["dist_count"] == 100.0

    def test_retrieval_percentiles(self) -> None:
        query_str = f"""MATCH (generic_metrics_distributions)
                    SELECT quantiles(0.5,0.9,0.95,0.99)(value) AS quants
                    BY project_id, org_id
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

        aggregation = data["data"][0]

        assert aggregation["org_id"] == self.org_id
        assert aggregation["project_id"] == self.project_id
        assert aggregation["quants"] == [2.0, approx(4.0), approx(4.0), approx(4.0)]
