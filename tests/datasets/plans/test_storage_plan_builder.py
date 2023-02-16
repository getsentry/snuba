from typing import List, Optional

import pytest

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToIPAddress,
    ColumnToMapping,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.entities.storage_selectors.errors import ErrorsQueryStorageSelector
from snuba.datasets.entities.storage_selectors.selector import (
    DefaultQueryStorageSelector,
)
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.datasets.plans.storage_plan_builder import StorageQueryPlanBuilder
from snuba.datasets.storage import EntityStorageConnection
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.request import Request

errors_translators = TranslationMappers(
    columns=[
        ColumnToMapping(None, "release", None, "tags", "sentry:release"),
        ColumnToMapping(None, "dist", None, "tags", "sentry:dist"),
        ColumnToMapping(None, "user", None, "tags", "sentry:user"),
        ColumnToIPAddress(None, "ip_address"),
        ColumnToColumn(None, "transaction", None, "transaction_name"),
        ColumnToColumn(None, "username", None, "user_name"),
        ColumnToColumn(None, "email", None, "user_email"),
        ColumnToMapping(
            None,
            "geo_country_code",
            None,
            "contexts",
            "geo.country_code",
            nullable=True,
        ),
        ColumnToMapping(
            None, "geo_region", None, "contexts", "geo.region", nullable=True
        ),
        ColumnToMapping(None, "geo_city", None, "contexts", "geo.city", nullable=True),
    ],
    subscriptables=[
        SubscriptableMapper(None, "tags", None, "tags"),
        SubscriptableMapper(None, "contexts", None, "contexts"),
    ],
)


TEST_CASES = [
    pytest.param(
        """
        MATCH (transactions)
        SELECT col1
        WHERE tags_key IN tuple('t1', 't2')
            AND finish_ts >= toDateTime('2021-01-01T00:00:00')
            AND finish_ts < toDateTime('2021-01-02T00:00:00')
            AND project_id = 1
        """,
        get_dataset("transactions"),
        get_entity(EntityKey.TRANSACTIONS).get_all_storage_connections(),
        DefaultQueryStorageSelector(),
        None,
        StorageSetKey.TRANSACTIONS,
        id="Single storage",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT col1
        WHERE tags_key IN tuple('t1', 't2')
            AND timestamp >= toDateTime('2021-01-01T00:00:00')
            AND timestamp < toDateTime('2021-01-02T00:00:00')
            AND project_id = 1
        """,
        get_dataset("events"),
        get_entity(EntityKey.EVENTS).get_all_storage_connections(),
        ErrorsQueryStorageSelector(),
        None,
        StorageSetKey.EVENTS,
        id="Multiple storages and selector",
    ),
]


@pytest.mark.parametrize(
    "snql_query, dataset, storage_connections, selector, partition_key_column_name, expected_storage_set_key",
    TEST_CASES,
)
def test_storage_query_plan_builder(
    snql_query: str,
    dataset: Dataset,
    storage_connections: List[EntityStorageConnection],
    selector: QueryStorageSelector,
    partition_key_column_name: Optional[str],
    expected_storage_set_key: StorageKey,
) -> None:
    query_plan_builder = StorageQueryPlanBuilder(
        storages=storage_connections,
        selector=selector,
        partition_key_column_name=partition_key_column_name,
    )
    query, snql_anonymized = parse_snql_query(str(snql_query), dataset)
    assert isinstance(query, Query)
    request = Request(
        id="a",
        original_body={"query": snql_query, "dataset": "transactions"},
        query=query,
        snql_anonymized=snql_anonymized,
        query_settings=HTTPQuerySettings(referrer="r"),
        attribution_info=AttributionInfo(
            get_app_id("blah"), {"blah": "blah"}, "blah", None, None, None
        ),
    )
    plan: ClickhouseQueryPlan = query_plan_builder.build_and_rank_plans(
        query=query, settings=request.query_settings
    )[0]

    assert plan.storage_set_key == expected_storage_set_key
