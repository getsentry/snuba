from __future__ import annotations

from snuba import settings
from snuba.clusters.storage_sets import DEV_STORAGE_SETS
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage
from snuba.query.allocation_policies import PassthroughPolicy


def get_allocation_policies() -> list[dict[str, str]]:

    storages = [
        storage
        for storage_key in sorted(
            get_all_storage_keys(), key=lambda storage_key: storage_key.value
        )
        if (storage := get_storage(storage_key)).get_storage_set_key()
        not in DEV_STORAGE_SETS
        or settings.ENABLE_DEV_FEATURES
    ]

    return [
        {
            "storage_name": storage.get_storage_key().value,
            "allocation_policy": storage.get_allocation_policy().config_key(),
        }
        for storage in storages
        if not isinstance(storage.get_allocation_policy(), PassthroughPolicy)
    ]


def get_storages_with_allocation_policies() -> list[str]:

    storages = [
        storage
        for storage_key in sorted(
            get_all_storage_keys(), key=lambda storage_key: storage_key.value
        )
        if (storage := get_storage(storage_key)).get_storage_set_key()
        not in DEV_STORAGE_SETS
        or settings.ENABLE_DEV_FEATURES
    ]

    return [
        storage.get_storage_key().value
        for storage in storages
        if not isinstance(storage.get_allocation_policy(), PassthroughPolicy)
    ]
