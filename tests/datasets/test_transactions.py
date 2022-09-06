from snuba import settings, state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.transactions import (
    TransactionsQueryStorageSelector,
    transaction_translator,
)
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings

RO_REFERRER = "RO_REFERRER"
RW_REFERRER = "RW_REFERRER"

STORAGE_SELECTOR = TransactionsQueryStorageSelector(mappers=transaction_translator)
STORAGE = get_storage(StorageKey.TRANSACTIONS)
STORAGE_RO = get_storage(StorageKey.TRANSACTIONS_RO)


def test_storage_selector_global_config() -> None:
    state.set_config("enable_transactions_readonly_table", True)

    query = Query(Entity(EntityKey.TRANSACTIONS, ColumnSet([])), selected_columns=[])

    assert (
        STORAGE_SELECTOR.select_storage(query, HTTPQuerySettings()).storage
        == STORAGE_RO
    )

    state.set_config("enable_transactions_readonly_table", False)
    assert (
        STORAGE_SELECTOR.select_storage(query, HTTPQuerySettings()).storage == STORAGE
    )


def test_storage_selector_query_settings() -> None:
    settings.TRANSACTIONS_DIRECT_TO_READONLY_REFERRERS = set([RO_REFERRER])

    query = Query(Entity(EntityKey.TRANSACTIONS, ColumnSet([])), selected_columns=[])

    assert (
        STORAGE_SELECTOR.select_storage(
            query, HTTPQuerySettings(referrer=RO_REFERRER)
        ).storage
        == STORAGE_RO
    )
    assert (
        STORAGE_SELECTOR.select_storage(
            query, HTTPQuerySettings(referrer=RW_REFERRER)
        ).storage
        == STORAGE
    )
