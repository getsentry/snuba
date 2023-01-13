from typing import List

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.sessions import (
    sessions_hourly_translators,
    sessions_raw_translators,
)
from snuba.datasets.entities.storage_selectors.selector import QueryStorageSelector
from snuba.datasets.entities.storage_selectors.sessions import (
    SessionsQueryStorageSelector,
)
from snuba.datasets.factory import get_dataset
from snuba.datasets.storage import EntityStorageConnection, Storage
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import SubscriptionQuerySettings
from snuba.query.snql.parser import parse_snql_query

TEST_CASES = [
    pytest.param(
        """
        MATCH (sessions)
        SELECT bucketed_started, users
        BY bucketed_started
        WHERE org_id = 1
        AND project_id IN tuple(1)
        AND started >= toDateTime('2022-01-01 01:00:00')
        AND started < toDateTime('2022-01-01 01:30:00')
        LIMIT 5000
        GRANULARITY 60
        """,
        get_dataset("sessions"),
        [
            EntityStorageConnection(
                get_storage(StorageKey.SESSIONS_HOURLY), sessions_hourly_translators
            ),
            EntityStorageConnection(
                get_writable_storage(StorageKey.SESSIONS_RAW),
                sessions_raw_translators,
                True,
            ),
        ],
        SessionsQueryStorageSelector(),
        get_storage(StorageKey.SESSIONS_RAW),
        id="Sessions storage selector",
    ),
    pytest.param(
        """
        MATCH (sessions)
        SELECT bucketed_started, users
        BY bucketed_started
        WHERE org_id = 1
        AND project_id IN tuple(1)
        AND started >= toDateTime('2022-01-01 01:00:00')
        AND started < toDateTime('2022-01-01 03:00:00')
        """,
        get_dataset("sessions"),
        [
            EntityStorageConnection(
                get_storage(StorageKey.SESSIONS_HOURLY), sessions_hourly_translators
            ),
            EntityStorageConnection(
                get_writable_storage(StorageKey.SESSIONS_RAW),
                sessions_raw_translators,
                True,
            ),
        ],
        SessionsQueryStorageSelector(),
        get_storage(StorageKey.SESSIONS_HOURLY),
        id="Sessions storage selector",
    ),
]


@pytest.mark.parametrize(
    "snql_query, dataset, storage_connections, selector, expected_storage",
    TEST_CASES,
)
def test_query_storage_selector(
    snql_query: str,
    dataset: Dataset,
    storage_connections: List[EntityStorageConnection],
    selector: QueryStorageSelector,
    expected_storage: Storage,
) -> None:
    query, _ = parse_snql_query(str(snql_query), dataset)
    assert isinstance(query, Query)
    selected_storage = selector.select_storage(
        query, SubscriptionQuerySettings(), storage_connections
    )
    assert selected_storage.storage == expected_storage
