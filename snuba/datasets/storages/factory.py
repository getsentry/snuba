from typing import Mapping

from snuba.datasets.cdc import CdcStorage
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
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


CDC_STORAGES: Mapping[str, CdcStorage] = {
    "groupedmessages": groupedmessages_storage,
    "groupassignees": groupassignees_storage,
}

WRITABLE_STORAGES: Mapping[str, WritableTableStorage] = {
    **CDC_STORAGES,
    "errors": errors_storage,
    "events": events_storage,
    "outcomes_raw": outcomes_raw_storage,
    "querylog": querylog_storage,
    "sessions_raw": sessions_raw_storage,
    "transactions": transactions_storage,
}

NON_WRITABLE_STORAGES: Mapping[str, ReadableTableStorage] = {
    "outcomes_hourly": outcomes_hourly_storage,
    "sessions_hourly": sessions_hourly_storage,
}

STORAGES: Mapping[str, ReadableTableStorage] = {**WRITABLE_STORAGES, **NON_WRITABLE_STORAGES}

def get_storage(storage_key: str) -> ReadableTableStorage:
    return STORAGES[storage_key]

def get_writable_storage(storage_key: str) -> WritableTableStorage:
    return WRITABLE_STORAGES[storage_key]

def get_cdc_storage(storage_key: str) -> CdcStorage:
    return CDC_STORAGES[storage_key]
