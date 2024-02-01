import itertools
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping, Tuple, Union

import pytest
from pytest import approx
from snuba_sdk import AliasedExpression, Function, Request
from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.entity import Entity
from snuba_sdk.expressions import Granularity
from snuba_sdk.query import Query

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.metrics_messages import InputType
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages

RETENTION_DAYS = 90
SNQL_ROUTE = "/generic_metrics/snql"


def utc_yesterday_12_15() -> datetime:
    return (datetime.utcnow() - timedelta(days=1)).replace(
        hour=12, minute=15, second=0, microsecond=0, tzinfo=timezone.utc
    )


placeholder_counter = 0


def gen_string() -> str:
    global placeholder_counter
    placeholder_counter += 1
    return "placeholder{:04d}".format(placeholder_counter)


SHARED_TAGS: Mapping[str, str] = {
    "65546": gen_string(),
    "9223372036854776010": gen_string(),
    "9223372036854776016": gen_string(),
    "9223372036854776020": gen_string(),
    "9223372036854776021": gen_string(),
    "9223372036854776022": gen_string(),
    "9223372036854776023": gen_string(),
    "9223372036854776026": gen_string(),
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


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGenericMetricsApiSets(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "generic_metrics_sets"

    @pytest.fixture(autouse=True)
    def setup_teardown(
        self, clickhouse_db: None, _build_snql_post_methods: Callable[[str], Any]
    ) -> None:
        self.post = _build_snql_post_methods

        self.write_storage = get_storage(StorageKey.GENERIC_METRICS_SETS_RAW)
        self.count = 10
        self.org_id = 1
        self.project_id = 2
        self.metric_id = 3
        self.base_time = utc_yesterday_12_15()
        self.sentry_received_timestamp = utc_yesterday_12_15()
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
        tags: Mapping[str, str],
        mapping_meta: Mapping[str, Mapping[str, str]],
        int_source: Iterable[int],
    ) -> None:
        assert isinstance(self.write_storage, WritableTableStorage)
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
                    "sentry_received_timestamp": self.sentry_received_timestamp.timestamp()
                    + n,
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
                    GRANULARITY 60
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
        assert len(data["data"]) == 1, data
        assert data["data"][0]["unique_values"] == self.unique_values

    def test_raw_tags(self) -> None:
        tag_key = 1337
        value_as_string = gen_string()
        new_set_unique_count = 10
        new_tag_values = {str(tag_key): value_as_string}
        new_mapping_meta = {"d": {str(tag_key): gen_string()}}
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
                    GRANULARITY 60
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
        assert len(data["data"]) == 1, data
        assert data["data"][0]["unique_values"] == new_set_unique_count


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGenericMetricsApiDistributions(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "generic_metrics_distributions"

    @pytest.fixture(autouse=True)
    def setup_teardown(
        self, clickhouse_db: None, _build_snql_post_methods: Callable[[str], Any]
    ) -> None:
        self.post = _build_snql_post_methods

        self.write_storage = get_storage(StorageKey.GENERIC_METRICS_DISTRIBUTIONS_RAW)
        self.count = 10
        self.org_id = 1
        self.project_id = 2
        self.metric_id = 3
        self.base_time = utc_yesterday_12_15()
        self.sentry_received_timestamp = utc_yesterday_12_15()
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
        self.hour_before_start_time = self.start_time - timedelta(hours=1)
        self.hour_after_start_time = self.start_time + timedelta(hours=1)

        self.generate_dists()

    def generate_dists(self) -> None:
        assert isinstance(self.write_storage, WritableTableStorage)
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
                    "sentry_received_timestamp": self.sentry_received_timestamp.timestamp()
                    + n,
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
                    GRANULARITY 60
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
                    GRANULARITY 60
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
        assert len(data["data"]) == 1, data

        aggregation = data["data"][0]

        assert aggregation["org_id"] == self.org_id
        assert aggregation["project_id"] == self.project_id
        assert aggregation["quants"] == [2.0, approx(4.0), approx(4.0), approx(4.0)]

    def test_arbitrary_granularity(self) -> None:
        query_str = f"""MATCH (generic_metrics_distributions)
                    SELECT quantiles(0.5,0.9,0.95,0.99)(value) AS quants, min(bucketed_time) AS min_time
                    BY project_id, org_id
                    WHERE org_id = {self.org_id}
                    AND project_id = {self.project_id}
                    AND metric_id = {self.metric_id}
                    AND timestamp >= toDateTime('{self.hour_before_start_time}')
                    AND timestamp < toDateTime('{self.hour_after_start_time}')
                    GRANULARITY 3600
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
        assert len(data["data"]) == 1, data

        aggregation = data["data"][0]
        smallest_time_bucket = datetime.strptime(
            aggregation["min_time"], "%Y-%m-%dT%H:%M:%S+00:00"
        )
        assert smallest_time_bucket.hour == 12
        assert smallest_time_bucket.minute == 0

    def test_tags_hash_map(self) -> None:
        shared_key = 65546  # pick a key from shared_values
        value = "65536"
        expected_value = SHARED_MAPPING_META["c"][value]
        query_str = f"""MATCH (generic_metrics_distributions)
                        SELECT count() AS thecount
                        WHERE tags_raw[{shared_key}] = '{expected_value}'
                        AND project_id = {self.project_id}
                        AND org_id = {self.org_id}
                        AND timestamp >= toDateTime('{self.start_time}')
                        AND timestamp < toDateTime('{self.end_time}')
                        """
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(
                {
                    "query": query_str,
                    "dataset": "generic_metrics",
                    "tenant_ids": {"referrer": "tests", "organization_id": 1},
                    "debug": True,
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 1, data
        assert "_raw_tags_hash" in data["sql"]


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGenericMetricsApiCounters(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "generic_metrics_counters"

    @pytest.fixture(autouse=True)
    def setup_post(
        self, clickhouse_db: None, _build_snql_post_methods: Callable[[str], Any]
    ) -> None:
        self.post = _build_snql_post_methods

        self.write_storage = get_storage(StorageKey.GENERIC_METRICS_COUNTERS_RAW)
        self.count = 10
        self.org_id = 1
        self.project_id = 2
        self.metric_id = 3
        self.base_time = utc_yesterday_12_15()
        self.sentry_received_timestamp = utc_yesterday_12_15()
        self.default_tags = SHARED_TAGS
        self.mapping_meta = SHARED_MAPPING_META

        self.use_case_id = "performance"
        self.start_time = self.base_time
        self.end_time = (
            self.base_time + timedelta(seconds=self.count) + timedelta(seconds=10)
        )
        self.hour_before_start_time = self.start_time - timedelta(hours=1)
        self.hour_after_start_time = self.start_time + timedelta(hours=1)
        self.generate_counters()

    def generate_counters(self) -> None:
        assert isinstance(self.write_storage, WritableTableStorage)
        rows = [
            self.write_storage.get_table_writer()
            .get_stream_loader()
            .get_processor()
            .process_message(
                {
                    "org_id": self.org_id,
                    "project_id": self.project_id,
                    "unit": "ms",
                    "type": InputType.COUNTER.value,
                    "value": 1.0,
                    "timestamp": self.base_time.timestamp() + n,
                    "tags": self.default_tags,
                    "metric_id": self.metric_id,
                    "retention_days": RETENTION_DAYS,
                    "mapping_meta": self.mapping_meta,
                    "use_case_id": self.use_case_id,
                    "sentry_received_timestamp": self.sentry_received_timestamp.timestamp()
                    + n,
                },
                KafkaMessageMetadata(0, 0, self.base_time),
            )
            for n in range(self.count)
        ]
        write_processed_messages(self.write_storage, [row for row in rows if row])

    def test_retrieval_basic(self) -> None:
        query_str = f"""MATCH (generic_metrics_counters)
                    SELECT sum(value) AS total BY project_id, org_id
                    WHERE org_id = {self.org_id}
                    AND project_id = {self.project_id}
                    AND metric_id = {self.metric_id}
                    AND timestamp >= toDateTime('{self.start_time}')
                    AND timestamp < toDateTime('{self.end_time}')
                    GRANULARITY 60
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

        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert data["data"][0]["total"] == 10.0

    def test_arbitrary_granularity(self) -> None:
        query_str = f"""MATCH (generic_metrics_counters)
                SELECT sum(value) AS total BY project_id, org_id
                WHERE org_id = {self.org_id}
                AND project_id = {self.project_id}
                AND metric_id = {self.metric_id}
                AND timestamp >= toDateTime('{self.hour_before_start_time}')
                AND timestamp < toDateTime('{self.hour_after_start_time}')
                GRANULARITY 3600
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
        assert len(data["data"]) == 1, data


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestOrgGenericMetricsApiCounters(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "generic_metrics_counters"

    @pytest.fixture(autouse=True)
    def setup_teardown(
        self, clickhouse_db: None, _build_snql_post_methods: Callable[[str], Any]
    ) -> None:
        self.post = _build_snql_post_methods

        self.count = 3600
        self.base_time = utc_yesterday_12_15()
        self.sentry_received_timestamp = utc_yesterday_12_15()

        self.start_time = self.base_time
        self.end_time = (
            self.base_time + timedelta(seconds=self.count) + timedelta(seconds=10)
        )
        self.hour_before_start_time = self.start_time - timedelta(hours=1)
        self.hour_after_start_time = self.start_time + timedelta(hours=1)
        self.mapping_meta = SHARED_MAPPING_META
        self.default_tags = SHARED_TAGS

        self.write_storage = get_storage(StorageKey.GENERIC_METRICS_COUNTERS_RAW)

        self.use_case_id = "performance"

        self.metric_id = 1001
        self.org_id = 101
        self.project_ids = [1, 2]  # 2 projects
        self.generate_counters()

    def generate_counters(self) -> None:
        assert isinstance(self.write_storage, WritableTableStorage)
        events = []
        for n in range(self.count):
            for p in self.project_ids:
                processed = (
                    self.write_storage.get_table_writer()
                    .get_stream_loader()
                    .get_processor()
                    .process_message(
                        (
                            {
                                "org_id": self.org_id,
                                "project_id": p,
                                "unit": "ms",
                                "type": InputType.COUNTER.value,
                                "value": 1.0,
                                "timestamp": self.base_time.timestamp() + n,
                                "tags": self.default_tags,
                                "metric_id": 1,
                                "retention_days": RETENTION_DAYS,
                                "mapping_meta": self.mapping_meta,
                                "use_case_id": self.use_case_id,
                                "sentry_received_timestamp": self.sentry_received_timestamp.timestamp()
                                + n,
                            }
                        ),
                        KafkaMessageMetadata(0, 0, self.base_time),
                    )
                )
                if processed:
                    events.append(processed)
        write_processed_messages(self.write_storage, events)

    def test_simple(self) -> None:
        query = Query(
            match=Entity("generic_org_metrics_counters"),
            select=[
                Function("sum", [Column("value")], "value"),
                Column("org_id"),
                Column("project_id"),
            ],
            groupby=[Column("org_id"), Column("project_id")],
            where=[
                Condition(Column("metric_id"), Op.EQ, 1),
                Condition(Column("timestamp"), Op.GTE, self.hour_before_start_time),
                Condition(Column("timestamp"), Op.LT, self.hour_after_start_time),
            ],
            granularity=Granularity(3600),
        )

        request = Request(
            dataset="generic_metrics",
            app_id="default",
            query=query,
            tenant_ids={"referrer": "tests", "organization_id": 1},
        )
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(request.to_dict()),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 2
        assert data["data"][0] == {"org_id": 101, "project_id": 1, "value": 3600.0}
        assert data["data"][1] == {"org_id": 101, "project_id": 2, "value": 3600.0}

    def test_raw_tags(self) -> None:
        """
        Tests that we can query raw tags
        """
        shared_key = 65546  # pick a key from shared_values
        tag_column_name = f"tags_raw[{shared_key}]"
        query = Query(
            match=Entity("generic_org_metrics_counters"),
            select=[
                Function("sum", [Column("value")], "value"),
                AliasedExpression(Column(tag_column_name), "tag_string"),
            ],
            groupby=[AliasedExpression(Column(tag_column_name), "tag_string")],
            where=[
                Condition(Column("metric_id"), Op.EQ, 1),
                Condition(Column("timestamp"), Op.GTE, self.hour_before_start_time),
                Condition(Column("timestamp"), Op.LT, self.hour_after_start_time),
            ],
            granularity=Granularity(3600),
        )

        request = Request(
            dataset="generic_metrics",
            app_id="default",
            query=query,
            tenant_ids={"referrer": "tests", "organization_id": 1},
        )
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(request.to_dict()),
        )
        data = json.loads(response.data)
        first_row = data["data"][0]
        assert first_row["tag_string"] == "placeholder0001"


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestOrgGenericMetricsApiGauges(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "generic_metrics_gauges"

    @pytest.fixture(autouse=True)
    def setup_teardown(
        self, clickhouse_db: None, _build_snql_post_methods: Callable[[str], Any]
    ) -> None:
        self.post = _build_snql_post_methods

        self.count = 3600
        self.base_time = utc_yesterday_12_15()
        self.sentry_received_timestamp = utc_yesterday_12_15()

        self.start_time = self.base_time
        self.end_time = (
            self.base_time + timedelta(seconds=self.count) + timedelta(seconds=10)
        )
        self.hour_before_start_time = self.start_time - timedelta(hours=1)
        self.hour_after_start_time = self.start_time + timedelta(hours=1)
        self.mapping_meta = SHARED_MAPPING_META
        self.default_tags = SHARED_TAGS

        self.write_storage = get_storage(StorageKey.GENERIC_METRICS_GAUGES_RAW)

        self.use_case_id = "performance"

        self.metric_id = 1001
        self.org_id = 101
        self.project_ids = [1, 2]  # 2 projects
        self.generate_gauges()

    def generate_gauges(self) -> None:
        assert isinstance(self.write_storage, WritableTableStorage)
        events = []
        for n in range(self.count):
            for p in self.project_ids:
                processed = (
                    self.write_storage.get_table_writer()
                    .get_stream_loader()
                    .get_processor()
                    .process_message(
                        (
                            {
                                "org_id": self.org_id,
                                "project_id": p,
                                "unit": "ms",
                                "type": InputType.GAUGE.value,
                                "value": {
                                    "min": 2.0,
                                    "max": 21.0,
                                    "sum": 25.0,
                                    "count": 3,
                                    "last": 4.0,
                                },
                                "timestamp": self.base_time.timestamp() + n,
                                "tags": self.default_tags,
                                "metric_id": self.metric_id,
                                "retention_days": RETENTION_DAYS,
                                "mapping_meta": self.mapping_meta,
                                "use_case_id": self.use_case_id,
                                "sentry_received_timestamp": self.sentry_received_timestamp.timestamp()
                                + n,
                            }
                        ),
                        KafkaMessageMetadata(0, 0, self.base_time),
                    )
                )
                if processed:
                    events.append(processed)
        write_processed_messages(self.write_storage, events)

    def test_simple_gauge(self) -> None:
        query = Query(
            match=Entity("generic_metrics_gauges"),
            select=[
                Function("last", [Column("value")], "value"),
                Column("org_id"),
                Column("project_id"),
            ],
            groupby=[Column("org_id"), Column("project_id")],
            where=[
                Condition(Column("metric_id"), Op.EQ, self.metric_id),
                Condition(Column("timestamp"), Op.GTE, self.hour_before_start_time),
                Condition(Column("timestamp"), Op.LT, self.hour_after_start_time),
                Condition(Column("org_id"), Op.EQ, self.org_id),
                Condition(Column("project_id"), Op.IN, self.project_ids),
            ],
            granularity=Granularity(3600),
        )

        request = Request(
            dataset="generic_metrics",
            app_id="default",
            query=query,
            tenant_ids={"referrer": "tests", "organization_id": self.org_id},
        )
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(request.to_dict()),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 2
        assert data["data"][0] == {
            "org_id": self.org_id,
            "project_id": 1,
            "value": 4.0,
        }
        assert data["data"][1] == {
            "org_id": self.org_id,
            "project_id": 2,
            "value": 4.0,
        }

    def test_bucketed_time_gauge(self) -> None:
        query = Query(
            match=Entity("generic_metrics_gauges"),
            select=[
                Column("bucketed_time"),
                Function("last", [Column("value")], "value"),
                Column("org_id"),
                Column("project_id"),
            ],
            groupby=[Column("org_id"), Column("project_id"), Column("bucketed_time")],
            where=[
                Condition(Column("metric_id"), Op.EQ, self.metric_id),
                Condition(Column("timestamp"), Op.GTE, self.hour_before_start_time),
                Condition(Column("timestamp"), Op.LT, self.hour_after_start_time),
                Condition(Column("org_id"), Op.EQ, self.org_id),
                Condition(Column("project_id"), Op.IN, self.project_ids),
            ],
            granularity=Granularity(3600),
        )

        request = Request(
            dataset="generic_metrics",
            app_id="default",
            query=query,
            tenant_ids={"referrer": "tests", "organization_id": self.org_id},
        )
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(request.to_dict()),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 4
