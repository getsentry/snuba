from typing import List

import pytest

from snuba import state
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToIPAddress,
    ColumnToMapping,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.entities.storage_selectors.errors import ErrorsQueryStorageSelector
from snuba.datasets.factory import get_dataset
from snuba.datasets.storage import (
    EntityStorageConnection,
    EntityStorageConnectionNotFound,
    Storage,
)
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
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
        MATCH (events)
        SELECT event_id
        WHERE project_id IN tuple(2 , 3)
        AND timestamp>=toDateTime('2021-01-01')
        AND timestamp<toDateTime('2021-01-02')
        """,
        get_dataset("events"),
        [
            EntityStorageConnection(
                get_storage(StorageKey.ERRORS_RO), errors_translators
            ),
            EntityStorageConnection(
                get_writable_storage(StorageKey.ERRORS), errors_translators, True
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
        SELECT event_id
        WHERE project_id IN tuple(2 , 3)
        AND timestamp>=toDateTime('2021-01-01')
        AND timestamp<toDateTime('2021-01-02')
        """,
        get_dataset("events"),
        [
            EntityStorageConnection(
                get_storage(StorageKey.ERRORS_RO), errors_translators
            ),
            EntityStorageConnection(
                get_writable_storage(StorageKey.ERRORS), errors_translators, True
            ),
        ],
        ErrorsQueryStorageSelector(),
        True,
        get_storage(StorageKey.ERRORS_RO),
        id="Errors storage selector",
    ),
]


@pytest.mark.parametrize(
    "snql_query, dataset, storage_connections, selector, use_readable, expected_storage",
    TEST_CASES,
)
@pytest.mark.redis_db
def test_query_storage_selector(
    snql_query: str,
    dataset: Dataset,
    storage_connections: List[EntityStorageConnection],
    selector: QueryStorageSelector,
    use_readable: bool,
    expected_storage: Storage,
) -> None:
    query, _ = parse_snql_query(str(snql_query), dataset)
    assert isinstance(query, Query)

    if use_readable:
        state.set_config("enable_events_readonly_table", True)
    selected_storage = selector.select_storage(
        query, HTTPQuerySettings(referrer="r"), storage_connections
    )
    assert selected_storage.storage == expected_storage


def test_assert_raises() -> None:
    query, _ = parse_snql_query(
        """
        MATCH (events)
        SELECT event_id
        WHERE project_id IN tuple(2 , 3)
        AND timestamp>=toDateTime('2021-01-01')
        AND timestamp<toDateTime('2021-01-02')
        """,
        dataset=get_dataset("events"),
    )
    with pytest.raises(EntityStorageConnectionNotFound):
        assert isinstance(query, Query)
        ErrorsQueryStorageSelector().select_storage(query, HTTPQuerySettings(), [])
