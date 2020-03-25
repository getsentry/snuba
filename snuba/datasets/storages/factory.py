from typing import Mapping

from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages.events import get_storage as get_event_storage
from snuba.datasets.storages.groupedmessages import get_storage as get_groupedmessages_storage
from snuba.datasets.storages.transactions import get_storage as get_transactions_storage

from snuba.datasets.storages.outcomes import (
    get_raw_storage as get_outcomes_raw_storage,
    get_materialized_storage as get_outcomes_hourly_storage,
)


WRITABLE_STORAGES: Mapping[str, WritableTableStorage] = {
    "events": get_event_storage(),
    "groupedmessages": get_groupedmessages_storage(),
    "outcomes_raw": get_outcomes_raw_storage(),
    "transactions": get_transactions_storage(),
}

NON_WRITABLE_STORAGES: Mapping[str, ReadableTableStorage] = {
    "outcomes_hourly": get_outcomes_hourly_storage(),
}

STORAGES: Mapping[str, ReadableTableStorage] = {**WRITABLE_STORAGES, **NON_WRITABLE_STORAGES}

def get_storage(storage_key: str) -> ReadableTableStorage:
    return STORAGES[storage_key]

def get_writable_storage(storage_key: str) -> WritableTableStorage:
    return WRITABLE_STORAGES[storage_key]
