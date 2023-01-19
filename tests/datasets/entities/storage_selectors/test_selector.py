from typing import List

import pytest

from snuba.clickhouse.translators.snuba.mappers import (
    FunctionNameMapper,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.entities.storage_selectors.selector import (
    DefaultQueryStorageSelector,
    QueryStorageSelectorError,
    SimpleQueryStorageSelector,
)
from snuba.datasets.entities.transactions import transaction_translator
from snuba.datasets.factory import get_dataset
from snuba.datasets.storage import EntityStorageConnection, Storage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query

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
        [
            EntityStorageConnection(
                get_storage(StorageKey.TRANSACTIONS), transaction_translator, True
            ),
        ],
        DefaultQueryStorageSelector(),
        get_storage(StorageKey.TRANSACTIONS),
        id="Default storage selector",
    ),
    pytest.param(
        """
        MATCH (generic_metrics_sets)
        SELECT uniq(value) AS unique_values BY project_id, org_id
        WHERE org_id = 1
        AND project_id = 1
        AND metric_id = 1
        AND timestamp >= toDateTime('2022-01-01')
        AND timestamp < toDateTime('2022-01-02')
        GRANULARITY 60
        """,
        get_dataset("generic_metrics"),
        [
            EntityStorageConnection(
                get_storage(StorageKey.GENERIC_METRICS_SETS),
                TranslationMappers(
                    functions=[
                        FunctionNameMapper("uniq", "uniqCombined64Merge"),
                        FunctionNameMapper("uniqIf", "uniqCombined64MergeIf"),
                    ],
                    subscriptables=[
                        SubscriptableMapper(
                            from_column_table=None,
                            from_column_name="tags_raw",
                            to_nested_col_table=None,
                            to_nested_col_name="tags",
                            value_subcolumn_name="raw_value",
                        ),
                        SubscriptableMapper(
                            from_column_table=None,
                            from_column_name="tags",
                            to_nested_col_table=None,
                            to_nested_col_name="tags",
                            value_subcolumn_name="indexed_value",
                        ),
                    ],
                ),
            ),
            EntityStorageConnection(
                get_storage(StorageKey.GENERIC_METRICS_SETS_RAW),
                TranslationMappers(
                    functions=[
                        FunctionNameMapper("uniq", "uniqCombined64Merge"),
                        FunctionNameMapper("uniqIf", "uniqCombined64MergeIf"),
                    ],
                    subscriptables=[
                        SubscriptableMapper(
                            from_column_table=None,
                            from_column_name="tags_raw",
                            to_nested_col_table=None,
                            to_nested_col_name="tags",
                            value_subcolumn_name="raw_value",
                        ),
                        SubscriptableMapper(
                            from_column_table=None,
                            from_column_name="tags",
                            to_nested_col_table=None,
                            to_nested_col_name="tags",
                            value_subcolumn_name="indexed_value",
                        ),
                    ],
                ),
                True,
            ),
        ],
        SimpleQueryStorageSelector(StorageKey.GENERIC_METRICS_SETS.value),
        get_storage(StorageKey.GENERIC_METRICS_SETS),
        id="Simple storage selector",
    ),
]


@pytest.mark.parametrize(
    "snql_query, dataset, storage_connections, selector, expected_storage",
    TEST_CASES,
)
def test_default_query_storage_selector(
    snql_query: str,
    dataset: Dataset,
    storage_connections: List[EntityStorageConnection],
    selector: QueryStorageSelector,
    expected_storage: Storage,
) -> None:
    query, _ = parse_snql_query(str(snql_query), dataset)
    assert isinstance(query, Query)

    selected_storage = selector.select_storage(
        query, HTTPQuerySettings(referrer="r"), storage_connections
    )
    assert selected_storage.storage == expected_storage


def test_assert_raises():
    query, _ = parse_snql_query(
        """ MATCH (generic_metrics_sets)
        SELECT uniq(value) AS unique_values BY project_id, org_id
        WHERE org_id = 1
        AND project_id = 1
        AND metric_id = 1
        AND timestamp >= toDateTime('2022-01-01')
        AND timestamp < toDateTime('2022-01-02')
        GRANULARITY 60
        """,
        get_dataset("generic_metrics"),
    )
    selector = SimpleQueryStorageSelector(StorageKey.GENERIC_METRICS_SETS.value)
    with pytest.raises(QueryStorageSelectorError):
        selector.select_storage(query, HTTPQuerySettings(), [])
