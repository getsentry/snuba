from snuba import settings, state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.transactions import (
    TransactionsQueryStorageSelector,
    transaction_translator,
)
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.request.request_settings import HTTPRequestSettings

RO_REFERRER = "RO_REFERRER"
RW_REFERRER = "RW_REFERRER"


def test_storage_selector_global_config() -> None:
    state.set_config("enable_transactions_readonly_table", True)

    storage_ro = get_storage(StorageKey.TRANSACTIONS_RO)

    query = Query(Entity(EntityKey.TRANSACTIONS, ColumnSet([])), selected_columns=[])

    storage_selector = TransactionsQueryStorageSelector(mappers=transaction_translator)
    assert (
        storage_selector.select_storage(query, HTTPRequestSettings()).storage
        == storage_ro
    )


def test_storage_selector_query_settings() -> None:
    settings.TRANSACTIONS_DIRECT_TO_READONLY_REFERRERS = set([RO_REFERRER])
    storage = get_storage(StorageKey.TRANSACTIONS)
    storage_ro = get_storage(StorageKey.TRANSACTIONS_RO)

    query = Query(Entity(EntityKey.TRANSACTIONS, ColumnSet([])), selected_columns=[])

    storage_selector = TransactionsQueryStorageSelector(mappers=transaction_translator)
    assert (
        storage_selector.select_storage(
            query, HTTPRequestSettings(referrer=RO_REFERRER)
        ).storage
        == storage_ro
    )
    assert (
        storage_selector.select_storage(
            query, HTTPRequestSettings(referrer=RW_REFERRER)
        ).storage
        == storage
    )
