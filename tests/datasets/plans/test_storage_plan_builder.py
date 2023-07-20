import importlib
from typing import Any, List, Optional

import pytest

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
from snuba.query.exceptions import QueryPlanException
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query

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


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
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
    plan: ClickhouseQueryPlan = query_plan_builder.build_and_rank_plans(
        query=query, settings=HTTPQuerySettings(referrer="r")
    )[0]
    assert plan.storage_set_key == expected_storage_set_key

    # make sure that an allocation policy on the storage is applied to the
    # from clause of the query. The query plan does not return the
    # chosen storage so this complicated check exists to make sure the chosen storage's
    # allocation policy exists on the data source
    ch_from_clause = plan.query.get_from_clause()
    correct_policy_assigned = False
    for storage_conn in storage_connections:
        storage = storage_conn.storage
        table_name = storage.get_schema().get_data_source().get_table_name()  # type: ignore
        if (
            table_name == ch_from_clause.table_name
            and ch_from_clause.allocation_policies == storage.get_allocation_policies()
        ):
            correct_policy_assigned = True
    assert correct_policy_assigned


@pytest.fixture(scope="function")
def temp_settings() -> Any:
    from snuba import settings

    yield settings
    importlib.reload(settings)


def test_storage_unavailable_error_in_plan_builder(temp_settings: Any) -> None:
    snql_query = """
        MATCH (events)
        SELECT col1
        WHERE tags_key IN tuple('t1', 't2')
            AND timestamp >= toDateTime('2021-01-01T00:00:00')
            AND timestamp < toDateTime('2021-01-02T00:00:00')
            AND project_id = 1
    """
    dataset = get_dataset("events")
    storage_connections = get_entity(EntityKey.EVENTS).get_all_storage_connections()
    selector = ErrorsQueryStorageSelector()
    query_plan_builder = StorageQueryPlanBuilder(
        storages=storage_connections,
        selector=selector,
        partition_key_column_name=None,
    )
    query, _ = parse_snql_query(str(snql_query), dataset)

    temp_settings.SUPPORTED_STATES = {}  # remove all supported states
    temp_settings.READINESS_STATE_FAIL_QUERIES = True

    assert isinstance(query, Query)
    with pytest.raises(
        QueryPlanException,
        match="The selected storage=errors is not available in this environment yet. To enable it, consider bumping the storage's readiness_state.",
    ):
        query_plan_builder.build_and_rank_plans(
            query=query, settings=HTTPQuerySettings(referrer="r")
        )
