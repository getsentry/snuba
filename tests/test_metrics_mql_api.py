from __future__ import annotations

import itertools
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, cast

import pytest
import simplejson as json
from snuba_sdk import (
    ArithmeticOperator,
    Column,
    Condition,
    Direction,
    Flags,
    Formula,
    Metric,
    MetricsQuery,
    MetricsScope,
    Op,
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

TRANSACTION_MRI = "d:transactions/duration@millisecond"
USE_CASE_ID = "performance"
RETENTION_DAYS = 90


def utc_yesterday_12_15() -> datetime:
    return (datetime.utcnow() - timedelta(days=1)).replace(
        hour=12, minute=15, second=0, microsecond=0, tzinfo=timezone.utc
    )


SHARED_TAGS = {
    "status_code": "200",
    "transaction": "t1",
}

# This is stored this way since that's how it's encoded in the message
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


def resolve_str(value: str) -> int:
    meta_lookup: dict[str, str] = {}
    for values in SHARED_MAPPING_META.values():
        for k, v in values.items():
            meta_lookup[v] = k

    if value not in meta_lookup:
        raise ValueError(f"Unknown value {value}, add it to the SHARED_MAPPING_META")

    return int(meta_lookup[value])


SET_CYCLE = itertools.cycle(range(0, 5))
DIST_CYCLE = itertools.cycle(range(0, 5))


@dataclass
class MetricFixture:
    entity: str
    type: InputType
    metric_id: int
    value: Callable[[], Any]


COUNTERS = MetricFixture(
    entity="generic_metrics_counters",
    type=InputType.COUNTER,
    metric_id=1067,
    value=lambda: 1.0,
)
DISTRIBUTIONS = MetricFixture(
    entity="generic_metrics_distributions",
    type=InputType.DISTRIBUTION,
    metric_id=1068,
    value=lambda: list(itertools.islice(DIST_CYCLE, 10)),
)
SETS = MetricFixture(
    entity="generic_metrics_sets",
    type=InputType.SET,
    metric_id=1083,
    value=lambda: list(itertools.islice(SET_CYCLE, 3)),
)
GAUGES = MetricFixture(
    entity="generic_metrics_gauges",
    type=InputType.GAUGE,
    metric_id=1071,
    value=lambda: {
        "min": 2.0,
        "max": 21.0,
        "sum": 25.0,
        "count": 3,
        "last": 4.0,
    },
)

DATASET = "generic_metrics"


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGenericMetricsMQLApi(BaseApiTest):
    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture(autouse=True)
    def setup_teardown(
        self,
        clickhouse_db: None,
    ) -> None:
        self.post = self.app.post
        self.mql_route = f"/{DATASET}/mql"

        # values for test data
        self.org_id = 101
        self.project_ids = [1, 2]  # 2 projects
        self.seconds = 180 * 60

        # Create tag values for the test data
        self.mapping_meta = SHARED_MAPPING_META

        self.tags = {
            str(resolve_str(k)): resolve_str(v) for k, v in SHARED_TAGS.items()
        }
        self.skew = timedelta(seconds=self.seconds)
        self.base_time = utc_yesterday_12_15()
        self.start_time = self.base_time - self.skew
        self.end_time = self.base_time + self.skew

        self.sentry_received_time = utc_yesterday_12_15() - timedelta(minutes=1)
        for fixture in [COUNTERS, SETS, DISTRIBUTIONS, GAUGES]:
            self.generate_metrics(fixture)

    def generate_metrics(self, fixture: MetricFixture) -> None:
        events = []
        storage = cast(
            WritableTableStorage,
            get_entity(EntityKey(fixture.entity)).get_writable_storage(),
        )
        for n in range(self.seconds)[::60]:
            for p in self.project_ids:
                processed = (
                    storage.get_table_writer()
                    .get_stream_loader()
                    .get_processor()
                    .process_message(
                        (
                            {
                                "org_id": self.org_id,
                                "project_id": p,
                                "use_case_id": USE_CASE_ID,
                                "unit": "ms",
                                "type": fixture.type.value,
                                "value": fixture.value(),
                                "timestamp": self.base_time.timestamp() + n,
                                "tags": self.tags,
                                "metric_id": fixture.metric_id,
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
        write_processed_messages(storage, events)

    def test_retrieval_basic(self) -> None:
        query = MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    TRANSACTION_MRI,
                    COUNTERS.metric_id,
                    COUNTERS.entity,
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
            indexer_mappings={
                TRANSACTION_MRI: COUNTERS.metric_id,
            },
        )
        response = self.app.post(
            self.mql_route,
            data=Request(
                dataset=DATASET,
                app_id="test",
                query=query,
                flags=Flags(debug=True),
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize_mql(),
        )
        data = json.loads(response.data)

        assert response.status_code == 200, data
        assert len(data["data"]) == 180, data

    def test_retrieval_complex(self) -> None:
        query = MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    TRANSACTION_MRI,
                    COUNTERS.metric_id,
                    COUNTERS.entity,
                ),
                aggregate="sum",
                filters=[
                    Condition(
                        Column("transaction"),
                        Op.EQ,
                        "t1",
                    )
                ],
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
            indexer_mappings={
                TRANSACTION_MRI: COUNTERS.metric_id,
                "transaction": resolve_str("transaction"),
                "status_code": resolve_str("status_code"),
            },
        )

        response = self.app.post(
            self.mql_route,
            data=Request(
                dataset=DATASET,
                app_id="test",
                query=query,
                flags=Flags(debug=True),
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize_mql(),
        )
        data = json.loads(response.data)

        assert response.status_code == 200, data
        rows = data["data"]
        assert len(rows) == 180, rows

        assert rows[0]["aggregate_value"] > 0
        assert rows[0]["status_code"] == "200"

    def test_interval_with_totals(self) -> None:
        query = MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    TRANSACTION_MRI,
                    COUNTERS.metric_id,
                    COUNTERS.entity,
                ),
                aggregate="sum",
                filters=[
                    Condition(
                        Column("transaction"),
                        Op.EQ,
                        "t1",
                    )
                ],
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
            indexer_mappings={
                TRANSACTION_MRI: COUNTERS.metric_id,
                "transaction": resolve_str("transaction"),
                "status_code": resolve_str("status_code"),
            },
        )

        response = self.app.post(
            self.mql_route,
            data=Request(
                dataset=DATASET,
                app_id="test",
                query=query,
                flags=Flags(debug=True),
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize_mql(),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        rows = data["data"]
        assert len(rows) == 180, rows

        assert rows[0]["aggregate_value"] > 0
        assert rows[0]["status_code"] == "200"
        assert (
            data["totals"]["aggregate_value"] > 180
        )  # Should be more than the number of data points

    def test_curried_functions(self) -> None:
        query = MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    TRANSACTION_MRI,
                    DISTRIBUTIONS.metric_id,
                    DISTRIBUTIONS.entity,
                ),
                aggregate="quantiles",
                aggregate_params=[0.5],
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
            indexer_mappings={
                TRANSACTION_MRI: DISTRIBUTIONS.metric_id,
                "transaction": resolve_str("transaction"),
                "status_code": resolve_str("status_code"),
            },
        )

        response = self.app.post(
            self.mql_route,
            data=Request(
                dataset=DATASET,
                app_id="test",
                query=query,
                flags=Flags(debug=True),
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize_mql(),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        rows = data["data"]
        assert len(rows) == 180, rows

        assert rows[0]["aggregate_value"][0] > 0
        assert rows[0]["status_code"] == "200"
        assert data["totals"]["aggregate_value"][0] == 2.0

    def test_total_orderby_functions(self) -> None:
        query = MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    TRANSACTION_MRI,
                    DISTRIBUTIONS.metric_id,
                    DISTRIBUTIONS.entity,
                ),
                aggregate="max",
                groupby=[Column("status_code")],
                filters=[Condition(Column("status_code"), Op.EQ, "200")],
            ),
            start=self.start_time,
            end=self.end_time,
            rollup=Rollup(granularity=60, totals=True, orderby=Direction.ASC),
            scope=MetricsScope(
                org_ids=[self.org_id],
                project_ids=self.project_ids,
                use_case_id=USE_CASE_ID,
            ),
            indexer_mappings={
                TRANSACTION_MRI: DISTRIBUTIONS.metric_id,
                "transaction": resolve_str("transaction"),
                "status_code": resolve_str("status_code"),
            },
        )

        response = self.app.post(
            self.mql_route,
            data=Request(
                dataset=DATASET,
                app_id="test",
                query=query,
                flags=Flags(debug=True),
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize_mql(),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        rows = data["data"]
        assert len(rows) == 1, rows

        assert rows[0]["aggregate_value"] == 4.0
        assert rows[0]["status_code"] == "200"

    def test_dots_in_mri_names(self) -> None:
        query = MetricsQuery(
            query=Timeseries(
                metric=Metric(
                    "transaction.duration",
                    "d:transactions/measurements.indexer_batch.payloads.len@none",
                    DISTRIBUTIONS.metric_id,
                    DISTRIBUTIONS.entity,
                ),
                aggregate="avg",
                aggregate_params=None,
                filters=[
                    Condition(
                        Column("status_code"),
                        Op.IN,
                        ["200", "400"],
                    )
                ],
                groupby=None,
            ),
            start=self.start_time,
            end=self.end_time,
            rollup=Rollup(interval=60, totals=None, orderby=None, granularity=60),
            scope=MetricsScope(
                org_ids=[self.org_id],
                project_ids=self.project_ids,
                use_case_id="transactions",
            ),
            indexer_mappings={
                "d:transactions/measurements.indexer_batch.payloads.len@none": DISTRIBUTIONS.metric_id,
                "status_code": resolve_str("status_code"),
            },
        )

        response = self.app.post(
            self.mql_route,
            data=Request(
                dataset=DATASET,
                app_id="test",
                query=query,
                flags=Flags(debug=True),
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize_mql(),
        )
        assert response.status_code == 200

    def test_simple_formula(self) -> None:
        query = MetricsQuery(
            query=Formula(
                ArithmeticOperator.PLUS.value,
                [
                    Timeseries(
                        metric=Metric(
                            "transaction.duration",
                            TRANSACTION_MRI,
                            DISTRIBUTIONS.metric_id,
                            DISTRIBUTIONS.entity,
                        ),
                        aggregate="avg",
                    ),
                    Timeseries(
                        metric=Metric(
                            "transaction.duration",
                            TRANSACTION_MRI,
                            DISTRIBUTIONS.metric_id,
                            DISTRIBUTIONS.entity,
                        ),
                        aggregate="avg",
                    ),
                ],
            ),
            start=self.start_time,
            end=self.end_time,
            rollup=Rollup(interval=60, totals=None, orderby=None, granularity=60),
            scope=MetricsScope(
                org_ids=[self.org_id],
                project_ids=self.project_ids,
                use_case_id=USE_CASE_ID,
            ),
            indexer_mappings={
                TRANSACTION_MRI: DISTRIBUTIONS.metric_id,
                "status_code": resolve_str("status_code"),
            },
        )

        response = self.app.post(
            self.mql_route,
            data=Request(
                dataset=DATASET,
                app_id="test",
                query=query,
                flags=Flags(debug=True),
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize_mql(),
        )
        assert response.status_code == 200, response.data
        data = json.loads(response.data)
        assert len(data["data"]) == 180, data

    @pytest.mark.xfail(
        reason="Bug in the SDK: snuba_sdk.formula.InvalidFormulaError: Formula parameters must group by the same columns"
    )
    def test_complex_formula(self) -> None:
        query = MetricsQuery(
            query=Formula(
                ArithmeticOperator.DIVIDE.value,
                [
                    Timeseries(
                        metric=Metric(
                            "transaction.duration",
                            TRANSACTION_MRI,
                            DISTRIBUTIONS.metric_id,
                            DISTRIBUTIONS.entity,
                        ),
                        aggregate="quantiles",
                        aggregate_params=[0.5],
                        filters=[
                            Condition(
                                Column("status_code"),
                                Op.IN,
                                ["200"],
                            )
                        ],
                        groupby=[Column("transaction")],
                    ),
                    Timeseries(
                        metric=Metric(
                            "transaction.duration",
                            TRANSACTION_MRI,
                            DISTRIBUTIONS.metric_id,
                            DISTRIBUTIONS.entity,
                        ),
                        aggregate="avg",
                        groupby=[Column("transaction")],
                    ),
                ],
            ),
            start=self.start_time,
            end=self.end_time,
            rollup=Rollup(interval=60, totals=None, orderby=None, granularity=60),
            scope=MetricsScope(
                org_ids=[self.org_id],
                project_ids=self.project_ids,
                use_case_id=USE_CASE_ID,
            ),
            indexer_mappings={
                TRANSACTION_MRI: DISTRIBUTIONS.metric_id,
                "status_code": resolve_str("status_code"),
                "transaction": resolve_str("transaction"),
            },
        )

        response = self.app.post(
            self.mql_route,
            data=Request(
                dataset=DATASET,
                app_id="test",
                query=query,
                flags=Flags(debug=True),
                tenant_ids={"referrer": "tests", "organization_id": self.org_id},
            ).serialize_mql(),
        )
        assert response.status_code == 200, response.data
        data = json.loads(response.data)
        assert len(data["data"]) == 180, data
