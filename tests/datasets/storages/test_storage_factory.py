from snuba.datasets.storage import Storage, WritableTableStorage
from snuba.datasets.storages.factory import (
    get_config_built_storages,
    get_storage,
    get_writable_storage_keys,
    initialize_storage_factory,
)
from snuba.datasets.storages.storage_key import StorageKey

initialize_storage_factory()

STORAGE_KEYS = [
    StorageKey.DISCOVER,
    StorageKey.ERRORS,
    StorageKey.ERRORS_RO,
    StorageKey.GROUPEDMESSAGES,
    StorageKey.GROUPASSIGNEES,
    StorageKey.METRICS_COUNTERS,
    StorageKey.ORG_METRICS_COUNTERS,
    StorageKey.METRICS_DISTRIBUTIONS,
    StorageKey.METRICS_SETS,
    StorageKey.METRICS_RAW,
    StorageKey.OUTCOMES_RAW,
    StorageKey.OUTCOMES_HOURLY,
    StorageKey.QUERYLOG,
    StorageKey.TRANSACTIONS,
    StorageKey.PROFILES,
    StorageKey.FUNCTIONS,
    StorageKey.FUNCTIONS_RAW,
    StorageKey.REPLAYS,
]


def test_get_storage() -> None:
    for storage_name in STORAGE_KEYS:
        storage = get_storage(storage_name)
        assert isinstance(storage, Storage)


def test_get_config_writable_storage_keys() -> None:
    # Check that a config writable storages is returned in get_writable_storage_keys()
    config_storage_keys = list(get_config_built_storages().keys())
    config_writable_storage_keys = [
        storage_key
        for storage_key in config_storage_keys
        if isinstance(get_storage(storage_key), WritableTableStorage)
    ]
    all_writable_storage_keys = get_writable_storage_keys()
    for config_writable_storage_key in config_writable_storage_keys:
        assert config_writable_storage_key in all_writable_storage_keys
