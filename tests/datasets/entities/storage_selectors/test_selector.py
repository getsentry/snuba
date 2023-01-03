from typing import List

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.storage_selectors.selector import (
    DefaultQueryStorageSelector,
    QueryStorageSelector,
)
from snuba.datasets.entities.transactions import transaction_translator
from snuba.datasets.factory import get_dataset
from snuba.datasets.storage import Storage, StorageAndMappers
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
            StorageAndMappers(
                get_storage(StorageKey.TRANSACTIONS), transaction_translator
            ),
        ],
        DefaultQueryStorageSelector(),
        get_storage(StorageKey.TRANSACTIONS),
        id="Default storage selector",
    ),
]


@pytest.mark.parametrize(
    "snql_query, dataset, storage_and_mappers, selector, expected_storage",
    TEST_CASES,
)
def test_query_storage_selector(
    snql_query: str,
    dataset: Dataset,
    storage_and_mappers: List[StorageAndMappers],
    selector: QueryStorageSelector,
    expected_storage: Storage,
) -> None:
    query, _ = parse_snql_query(str(snql_query), dataset)
    assert isinstance(query, Query)

    selected_storage = selector.select_storage(
        query, HTTPQuerySettings(referrer="r"), storage_and_mappers
    )
    assert selected_storage.storage == expected_storage
