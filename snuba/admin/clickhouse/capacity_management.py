from __future__ import annotations

from snuba import settings
from snuba.clusters.storage_sets import DEV_STORAGE_SETS
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage


def get_allocation_policies() -> list[dict[str, str]]:

    return [
        {
            "storage_name": storage_key.value,
            "allocation_policy": storage.get_allocation_policy().config_key(),
        }
        for storage_key in sorted(
            get_all_storage_keys(), key=lambda storage_key: storage_key.value
        )
        if (storage := get_storage(storage_key)).get_storage_set_key()
        not in DEV_STORAGE_SETS
        or settings.ENABLE_DEV_FEATURES
    ]
