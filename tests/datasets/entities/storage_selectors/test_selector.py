from typing import List

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.entities.storage_selectors.selector import (
    DefaultQueryStorageSelector,
    QueryStorageSelectorError,
    SimpleQueryStorageSelector,
)
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
        SELECT event_id
        WHERE tags_key IN tuple('t1', 't2')
            AND finish_ts >= toDateTime('2021-01-01T00:00:00')
            AND finish_ts < toDateTime('2021-01-02T00:00:00')
            AND project_id = 1
        """,
        get_dataset("transactions"),
        get_entity(EntityKey.TRANSACTIONS).get_all_storage_connections(),
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
        get_entity(EntityKey.GENERIC_METRICS_SETS).get_all_storage_connections(),
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


def test_assert_raises() -> None:
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
        assert isinstance(query, Query)
        selector.select_storage(query, HTTPQuerySettings(), [])
