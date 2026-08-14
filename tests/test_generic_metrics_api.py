import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
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
        hour=12, minute=15, second=0, microsecond=0, tzinfo=UTC
    )


placeholder_counter = 0


def gen_string() -> str:
    global placeholder_counter
    placeholder_counter += 1
    return f"placeholder{placeholder_counter:04d}"


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


@pytest.mark.genmetrics_db
@pytest.mark.redis_db
class TestGenericMetricsApiCounters(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> str | tuple[str, str]:
        return "generic_metrics_counters"

    @pytest.fixture(autouse=True)
    def setup_post(
        self, genmetrics_db: None, _build_snql_post_methods: Callable[[str], Any]
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
        self.end_time = self.base_time + timedelta(seconds=self.count) + timedelta(seconds=10)
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
                    "sentry_received_timestamp": self.sentry_received_timestamp.timestamp() + n,
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

    def test_retrieval_basic_weighted(self) -> None:
        query_str = f"""MATCH (generic_metrics_counters)
                    SELECT sum_weighted(value) AS total BY project_id, org_id
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
        assert data["data"][0]["total"] == 20.0

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


@pytest.mark.genmetrics_db
@pytest.mark.redis_db
class TestOrgGenericMetricsApiCounters(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> str | tuple[str, str]:
        return "generic_metrics_counters"

    @pytest.fixture(autouse=True)
    def setup_teardown(
        self, genmetrics_db: None, _build_snql_post_methods: Callable[[str], Any]
    ) -> None:
        self.post = _build_snql_post_methods

        self.count = 3600
        self.base_time = utc_yesterday_12_15()
        self.sentry_received_timestamp = utc_yesterday_12_15()

        self.start_time = self.base_time
        self.end_time = self.base_time + timedelta(seconds=self.count) + timedelta(seconds=10)
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
