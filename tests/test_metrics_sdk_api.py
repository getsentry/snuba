from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Generator, Tuple, Union, cast

import pytest
import simplejson as json
from snuba_sdk import (
    AliasedExpression,
    Column,
    Condition,
    Metric,
    MetricsQuery,
    MetricsScope,
    Op,
    Rollup,
    Timeseries,
)

from snuba import state
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.metrics_messages import InputType
from snuba.datasets.storage import WritableTableStorage
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages

TransactionMRI = "d:transactions/duration@millisecond"
UseCaseID = "transactions"
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


def teardown_common() -> None:
    # Reset rate limits
    state.delete_config("global_concurrent_limit")
    state.delete_config("global_per_second_limit")
    state.delete_config("project_concurrent_limit")
    state.delete_config("project_concurrent_limit_1")
    state.delete_config("project_per_second_limit")
    state.delete_config("date_align_seconds")


def utc_yesterday_12_15() -> datetime:
    return (datetime.utcnow() - timedelta(days=1)).replace(
        hour=12, minute=15, second=0, microsecond=0, tzinfo=timezone.utc
    )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestMetricsSdkApiCounters(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "metrics_counters"

    @pytest.fixture(autouse=True)
    def setup_teardown(
        self, _build_snql_post_methods: Callable[[str], Any], clickhouse_db: None
    ) -> Generator[None, None, None]:
        self.post = _build_snql_post_methods

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

        self.base_time = utc_yesterday_12_15()
        self.sentry_received_time = utc_yesterday_12_15() - timedelta(minutes=1)
        self.storage = cast(
            WritableTableStorage,
            get_entity(EntityKey.METRICS_COUNTERS).get_writable_storage(),
        )
        self.generate_counters()

        yield

        teardown_common()

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
                                "use_case_id": UseCaseID,
                                "unit": "ms",
                                "type": InputType.COUNTER.value,
                                "value": 1.0,
                                "timestamp": self.base_time.timestamp() + n,
                                "tags": self.default_tags,
                                "metric_id": self.metric_id,
                                "retention_days": RETENTION_DAYS,
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

    def test_retrieval_basic(self) -> None:
        start_time = self.base_time - self.skew
        end_time = self.base_time + self.skew

        query = MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    TransactionMRI,
                    self.metric_id,
                    "metrics_counters",
                ),
                aggregate="sum",
                # filters=[Condition(Column("status_code"), Op.EQ, "500")],
                # groupby=[Column("transaction")],
            ),
            # filters=[Condition(Column("device"), Op.EQ, "BlackBerry")],
            start=start_time,
            end=end_time,
            rollup=Rollup(interval=60, granularity=60),
            scope=MetricsScope(
                org_ids=[self.org_id],
                project_ids=self.project_ids,
                use_case_id=UseCaseID,
            ),
        )

        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps({"query": query.serialize(), "dataset": "metrics"}),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 180, data

    def test_retrieval_complex(self) -> None:
        start_time = self.base_time - self.skew
        end_time = self.base_time + self.skew

        query = MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    TransactionMRI,
                    self.metric_id,
                    "metrics_counters",
                ),
                aggregate="sum",
                filters=[Condition(Column(f"tags[{TAG_1_KEY}]"), Op.EQ, "91")],
                groupby=[AliasedExpression(Column(f"tags[{TAG_2_KEY}]"), "project_id")],
            ),
            start=start_time,
            end=end_time,
            rollup=Rollup(interval=60, granularity=60),
            scope=MetricsScope(
                org_ids=[self.org_id],
                project_ids=self.project_ids,
                use_case_id=UseCaseID,
            ),
        )

        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps({"query": query.serialize(), "dataset": "metrics"}),
        )
        data = json.loads(response.data)

        assert response.status_code == 200, data
        rows = data["data"]
        assert len(rows) == 180, rows

        assert rows[0]["aggregate_value"] == 120.0
        assert rows[0]["project_id"] == 134

    def test_interval_with_totals(self) -> None:
        start_time = self.base_time - self.skew
        end_time = self.base_time + self.skew

        query = MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    TransactionMRI,
                    self.metric_id,
                    "metrics_counters",
                ),
                aggregate="sum",
                filters=[Condition(Column(f"tags[{TAG_1_KEY}]"), Op.EQ, "91")],
                groupby=[AliasedExpression(Column(f"tags[{TAG_2_KEY}]"), "project_id")],
            ),
            start=start_time,
            end=end_time,
            rollup=Rollup(interval=60, granularity=60, totals=True),
            scope=MetricsScope(
                org_ids=[self.org_id],
                project_ids=self.project_ids,
                use_case_id=UseCaseID,
            ),
        )

        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps({"query": query.serialize(), "dataset": "metrics"}),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        rows = data["data"]
        assert len(rows) == 180, rows

        assert rows[0]["aggregate_value"] == 120.0
        assert rows[0]["project_id"] == 134
        assert data["totals"]["aggregate_value"] == 21600.0
