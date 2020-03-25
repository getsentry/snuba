from typing import Mapping

from snuba.datasets.storage import Storage
from snuba.datasets.storages.events import get_storage as get_event_storage
from snuba.datasets.storages.groupedmessages import get_storage as get_groupedmessages_storage
from snuba.datasets.storages.transactions import get_storage as get_transactions_storage


STORAGES: Mapping[str, Storage] = {
    "events": get_event_storage(),
    "groupedmessages": get_groupedmessages_storage(),
    "transactions": get_transactions_storage(),
}

def get_storage(storage_key: str) -> Storage:
    return STORAGES[storage_key]
