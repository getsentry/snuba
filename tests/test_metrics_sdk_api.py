from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Tuple, Union, cast

import pytest
import simplejson as json
from snuba_sdk import (
    Column,
    Condition,
    Entity,
    Metric,
    MetricsQuery,
    MetricsScope,
    Op,
    Query,
    Request,
    Rollup,
    Timeseries,
)

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.metrics_messages import InputType
from snuba.datasets.storage import WritableTableStorage
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages

COUNTERS_MRI = "c:transactions/count_per_root_project@none"
USE_CASE_ID = "performance"
RETENTION_DAYS = 90


def utc_yesterday_12_15() -> datetime:
    return (datetime.utcnow() - timedelta(days=1)).replace(
        hour=12, minute=15, second=0, microsecond=0, tzinfo=timezone.utc
    )


SHARED_TAGS: dict[str, str | int] = {
    "65546": 65536,
    "9223372036854776010": 65593,
}

SHARED_MAPPING_META = {
    "c": {
        "65546": "transaction",
        "65536": "t1",
        "65593": "200",
    },
    "h": {
        "9223372036854776010": "status_code",
    },
}


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGenericMetricsSdkApiCounters(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "generic_metrics_counters"

    @pytest.fixture
    def test_dataset(self) -> str:
        return "generic_metrics"

    @pytest.fixture
    def tag_column(self) -> str:
        return "tags_raw"

    @pytest.fixture
    def tag_value_indexed(self) -> bool:
        return False

    @pytest.fixture(autouse=True)
    def setup_teardown(
        self,
        _build_snql_post_methods: Callable[[str], Any],
        clickhouse_db: None,
        test_entity: str,
        test_dataset: str,
        tag_value_indexed: bool,
    ) -> None:
        self.post = _build_snql_post_methods
        self.mql_route = f"/{test_dataset}/mql"
        # values for test data
        self.metric_id = 1001
        self.org_id = 101
        self.project_ids = [1, 2]  # 2 projects
        self.seconds = 180 * 60

        self.mapping_meta = SHARED_MAPPING_META
        self.default_tags: dict[str, str | int] = SHARED_TAGS

        def intstr(v: str | int) -> str | int:
            try:
                return int(v)
            except ValueError:
                return str(v)

        self.indexer_mappings = {}
        for mapping in self.mapping_meta.values():
            self.indexer_mappings.update(
                {str(v): intstr(k) for k, v in mapping.items()}
            )

        self.indexer_mappings.update(
            {"transaction.duration": COUNTERS_MRI, COUNTERS_MRI: self.metric_id}
        )
        # This is a little confusing, but these values are the ones that should be used in the tests
        # Depending on the dataset, the values could be raw strings or indexed ints, so handle those cases
        if tag_value_indexed:
            self.tags: list[tuple[str, str | int]] = [
                (k, v) for k, v in self.default_tags.items()
            ]
        else:
            mapping = {}
            for v in self.mapping_meta.values():
                mapping.update(v)

            self.tags = [(k, mapping[str(v)]) for k, v in self.default_tags.items()]
            self.default_tags = {k: mapping[str(v)] for (k, v) in SHARED_TAGS.items()}

        self.skew = timedelta(seconds=self.seconds)
        self.base_time = utc_yesterday_12_15()
        self.start_time = self.base_time - self.skew
        self.end_time = self.base_time + self.skew

        self.sentry_received_time = utc_yesterday_12_15() - timedelta(minutes=1)
        self.storage = cast(
            WritableTableStorage,
            get_entity(EntityKey(test_entity)).get_writable_storage(),
        )
        self.generate_counters()

    def generate_counters(self) -> None:
        events = []
        for n in range(self.seconds)[::60]:
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
                                "use_case_id": USE_CASE_ID,
                                "unit": "ms",
                                "type": InputType.COUNTER.value,
                                "value": 1.0,
                                "timestamp": self.base_time.timestamp() + n,
                                "tags": self.default_tags,
                                "metric_id": self.metric_id,
                                "retention_days": RETENTION_DAYS,
                                "mapping_meta": self.mapping_meta,
                                "sentry_received_timestamp": self.sentry_received_time.timestamp()
                                + n,
                            }
                        ),
                        KafkaMessageMetadata(0, 0, self.base_time),
                    )
                )
                if processed:
                    events.append(processed)
        write_processed_messages(self.storage, events)

    def test_retrieval_basic(self, test_dataset: str) -> None:
        query = MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    COUNTERS_MRI,
                    self.metric_id,
                ),
                aggregate="sum",
            ),
            start=self.start_time,
            end=self.end_time,
            rollup=Rollup(interval=60, granularity=60),
            scope=MetricsScope(
                org_ids=[self.org_id],
                project_ids=self.project_ids,
                use_case_id=USE_CASE_ID,
            ),
            indexer_mappings=self.indexer_mappings,
        )
        response = self.app.post(
            self.mql_route,
            data=Request(
                query=query,
                dataset=test_dataset,
                app_id="test",
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize(),
        )
        data = json.loads(response.data)

        assert response.status_code == 200, data
        assert len(data["data"]) == 180, data

    def test_retrieval_complex(self, test_dataset: str, tag_column: str) -> None:
        query = MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    COUNTERS_MRI,
                    self.metric_id,
                ),
                aggregate="sum",
                filters=[Condition(Column("transaction"), Op.EQ, "t1")],
                groupby=[Column("status_code")],
            ),
            start=self.start_time,
            end=self.end_time,
            rollup=Rollup(interval=60, granularity=60),
            scope=MetricsScope(
                org_ids=[self.org_id],
                project_ids=self.project_ids,
                use_case_id=USE_CASE_ID,
            ),
            indexer_mappings=self.indexer_mappings,
        )

        response = self.app.post(
            self.mql_route,
            data=Request(
                query=query,
                dataset=test_dataset,
                app_id="test",
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize(),
        )
        data = json.loads(response.data)

        assert response.status_code == 200, data
        rows = data["data"]
        assert len(rows) == 180, rows

        assert rows[0]["aggregate_value"] > 0
        assert rows[0]["status_code"] == self.tags[1][1]

    def test_interval_with_totals(self, test_dataset: str, tag_column: str) -> None:
        query = MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    COUNTERS_MRI,
                    self.metric_id,
                ),
                aggregate="sum",
                filters=[Condition(Column("transaction"), Op.EQ, "t1")],
                groupby=[Column("status_code")],
            ),
            start=self.start_time,
            end=self.end_time,
            rollup=Rollup(interval=60, granularity=60, totals=True),
            scope=MetricsScope(
                org_ids=[self.org_id],
                project_ids=self.project_ids,
                use_case_id=USE_CASE_ID,
            ),
            indexer_mappings=self.indexer_mappings,
        )

        response = self.app.post(
            self.mql_route,
            data=Request(
                query=query,
                dataset=test_dataset,
                app_id="test",
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize(),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        rows = data["data"]
        assert len(rows) == 180, rows

        assert rows[0]["aggregate_value"] > 0
        assert rows[0]["status_code"] == self.tags[1][1]
        assert (
            data["totals"]["aggregate_value"] > 180
        )  # Should be more than the number of data points

    def test_tag_key_value(
        self, test_entity: str, test_dataset: str, tag_column: str
    ) -> None:
        query = (
            Query(Entity(test_entity))
            .set_select([Column("tags.key"), Column("tags.raw_value")])
            .set_groupby([Column("tags.key"), Column("tags.raw_value")])
            .set_where(
                [
                    Condition(Column("org_id"), Op.EQ, self.org_id),
                    Condition(Column("project_id"), Op.IN, self.project_ids),
                    Condition(Column("metric_id"), Op.EQ, self.metric_id),
                    Condition(Column("timestamp"), Op.GTE, self.start_time),
                    Condition(Column("timestamp"), Op.LT, self.end_time),
                ]
            )
        )

        response = self.app.post(
            f"{test_dataset}/snql",  # Not an MQL query
            data=Request(
                dataset=test_dataset,
                app_id="test",
                query=query,
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize(),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        rows = data["data"]
        assert len(rows) == 1, rows
        assert rows[0] == {
            "tags.key": [int(k) for k in SHARED_TAGS.keys()],
            "tags.raw_value": ["t1", "200"],
        }

    def test_raw_mql_string(self, test_dataset: str, tag_column: str) -> None:
        query = MetricsQuery(
            query=f"((sum({COUNTERS_MRI}{{transaction:t1}}) / sum({COUNTERS_MRI})){{transaction:t2}} + sum({COUNTERS_MRI}){{transaction:t3}}) by transaction",
            start=self.start_time,
            end=self.end_time,
            rollup=Rollup(interval=60, granularity=60, totals=True),
            scope=MetricsScope(
                org_ids=[self.org_id],
                project_ids=self.project_ids,
                use_case_id=USE_CASE_ID,
            ),
            indexer_mappings=self.indexer_mappings,
        )

        response = self.app.post(
            self.mql_route,
            data=Request(
                query=query,
                dataset=test_dataset,
                app_id="test",
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize(),
        )
        data = json.loads(response.data)

        assert response.status_code == 200, data
        rows = data["data"]
        assert len(rows) >= 180, rows

        assert math.isnan(rows[0]["aggregate_value"])  # division by zero


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestMetricsSdkApiCounters(TestGenericMetricsSdkApiCounters):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "metrics_counters"

    @pytest.fixture
    def test_dataset(self) -> str:
        return "metrics"

    @pytest.fixture
    def tag_column(self) -> str:
        return "tags"

    @pytest.fixture
    def tag_value_indexed(self) -> bool:
        return True

    @pytest.mark.skip("tags.raw_value not in metrics")
    def test_tag_key_value(
        self, test_entity: str, test_dataset: str, tag_column: str
    ) -> None:
        pass
