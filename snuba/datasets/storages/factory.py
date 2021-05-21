from typing import Mapping

from snuba import settings
from snuba.datasets.cdc import CdcStorage
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.discover import storage as discover_storage
from snuba.datasets.storages.errors import storage as errors_storage
from snuba.datasets.storages.errors_ro import storage as errors_ro_storage
from snuba.datasets.storages.events import storage as events_storage
from snuba.datasets.storages.events_ro import storage as events_ro_storage
from snuba.datasets.storages.groupassignees import storage as groupassignees_storage
from snuba.datasets.storages.groupedmessages import storage as groupedmessages_storage
from snuba.datasets.storages.metrics import buckets_storage as metrics_buckets_storage
from snuba.datasets.storages.metrics import sets_storage as metrics_sets_storage
from snuba.datasets.storages.outcomes import (
    materialized_storage as outcomes_hourly_storage,
)
from snuba.datasets.storages.outcomes import raw_storage as outcomes_raw_storage
from snuba.datasets.storages.querylog import storage as querylog_storage
from snuba.datasets.storages.sessions import (
    materialized_storage as sessions_hourly_storage,
)
from snuba.datasets.storages.sessions import raw_storage as sessions_raw_storage
from snuba.datasets.storages.spans import storage as spans_storage
from snuba.datasets.storages.transactions import storage as transactions_storage

DEV_CDC_STORAGES: Mapping[StorageKey, CdcStorage] = {}

CDC_STORAGES: Mapping[StorageKey, CdcStorage] = {
    **{
        storage.get_storage_key(): storage
        for storage in [groupedmessages_storage, groupassignees_storage]
    },
    **(DEV_CDC_STORAGES if settings.ENABLE_DEV_FEATURES else {}),
}

DEV_WRITABLE_STORAGES: Mapping[StorageKey, WritableTableStorage] = {
    metrics_buckets_storage.get_storage_key(): metrics_buckets_storage,
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
            spans_storage,
        ]
    },
    **(DEV_WRITABLE_STORAGES if settings.ENABLE_DEV_FEATURES else {}),
}

DEV_NON_WRITABLE_STORAGES: Mapping[StorageKey, ReadableTableStorage] = {
    metrics_sets_storage.get_storage_key(): metrics_sets_storage
}

NON_WRITABLE_STORAGES: Mapping[StorageKey, ReadableTableStorage] = {
    **{
        storage.get_storage_key(): storage
        for storage in [
            discover_storage,
            errors_ro_storage,
            events_ro_storage,
            outcomes_hourly_storage,
            sessions_hourly_storage,
        ]
    },
    **(DEV_NON_WRITABLE_STORAGES if settings.ENABLE_DEV_FEATURES else {}),
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
