from typing import List

import pytest

from snuba import state
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.events import errors_translators
from snuba.datasets.entities.storage_selectors.errors import ErrorsQueryStorageSelector
from snuba.datasets.entities.storage_selectors.selector import QueryStorageSelector
from snuba.datasets.factory import get_dataset
from snuba.datasets.storage import Storage, StorageAndMappers
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query

TEST_CASES = [
    pytest.param(
        """
        MATCH (events)
        SELECT col1
        WHERE project_id IN tuple(2 , 3)
        AND timestamp>=toDateTime('2021-01-01')
        AND timestamp<toDateTime('2021-01-02')
        """,
        get_dataset("events"),
        [
            StorageAndMappers(get_storage(StorageKey.ERRORS_RO), errors_translators),
            StorageAndMappers(
                get_writable_storage(StorageKey.ERRORS), errors_translators
            ),
        ],
        ErrorsQueryStorageSelector(),
        False,
        get_storage(StorageKey.ERRORS),
        id="Errors storage selector",
    ),
    pytest.param(
        """
        MATCH (events)
        SELECT col1
        WHERE project_id IN tuple(2 , 3)
        AND timestamp>=toDateTime('2021-01-01')
        AND timestamp<toDateTime('2021-01-02')
        """,
        get_dataset("events"),
        [
            StorageAndMappers(get_storage(StorageKey.ERRORS_RO), errors_translators),
            StorageAndMappers(
                get_writable_storage(StorageKey.ERRORS), errors_translators
            ),
        ],
        ErrorsQueryStorageSelector(),
        True,
        get_storage(StorageKey.ERRORS_RO),
        id="Errors storage selector",
    ),
]


@pytest.mark.parametrize(
    "snql_query, dataset, storage_and_mappers, selector, use_readable, expected_storage",
    TEST_CASES,
)
def test_query_storage_selector(
    snql_query: str,
    dataset: Dataset,
    storage_and_mappers: List[StorageAndMappers],
    selector: QueryStorageSelector,
    use_readable: bool,
    expected_storage: Storage,
) -> None:
    query, _ = parse_snql_query(str(snql_query), dataset)
    assert isinstance(query, Query)

    if use_readable:
        state.set_config("enable_events_readonly_table", True)
    selected_storage = selector.select_storage(
        query, HTTPQuerySettings(referrer="r"), storage_and_mappers
    )
    assert selected_storage.storage == expected_storage
