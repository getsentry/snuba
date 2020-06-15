from typing import Mapping

from snuba.datasets.cdc import CdcStorage
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.errors import storage as errors_storage
from snuba.datasets.storages.events import storage as events_storage
from snuba.datasets.storages.groupassignees import storage as groupassignees_storage
from snuba.datasets.storages.groupedmessages import storage as groupedmessages_storage
from snuba.datasets.storages.outcomes import (
    raw_storage as outcomes_raw_storage,
    materialized_storage as outcomes_hourly_storage,
)
from snuba.datasets.storages.querylog import storage as querylog_storage
from snuba.datasets.storages.sessions import (
    raw_storage as sessions_raw_storage,
    materialized_storage as sessions_hourly_storage,
)
from snuba.datasets.storages.transactions import storage as transactions_storage


CDC_STORAGES: Mapping[StorageKey, CdcStorage] = {
    storage.get_storage_key(): storage
    for storage in [groupedmessages_storage, groupassignees_storage]
}

WRITABLE_STORAGES: Mapping[StorageKey, WritableTableStorage] = {
    **CDC_STORAGES,
    **{
        storage.get_storage_key(): storage
        for storage in [
            errors_storage,
            events_storage,
            outcomes_raw_storage,
            querylog_storage,
            sessions_raw_storage,
            transactions_storage,
        ]
    },
}

NON_WRITABLE_STORAGES: Mapping[StorageKey, ReadableTableStorage] = {
    storage.get_storage_key(): storage
    for storage in [outcomes_hourly_storage, sessions_hourly_storage]
}

STORAGES: Mapping[StorageKey, ReadableTableStorage] = {
    **WRITABLE_STORAGES,
    **NON_WRITABLE_STORAGES,
}


def get_storage(storage_key: StorageKey) -> ReadableTableStorage:
    return STORAGES[storage_key]


def get_writable_storage(storage_key: StorageKey) -> WritableTableStorage:
    return WRITABLE_STORAGES[storage_key]


def get_cdc_storage(storage_key: StorageKey) -> CdcStorage:
    return CDC_STORAGES[storage_key]
