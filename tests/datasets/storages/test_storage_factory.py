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
    StorageKey.SESSIONS_RAW,
    StorageKey.SESSIONS_HOURLY,
    StorageKey.ORG_SESSIONS,
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


def test_get_writable_storage_keys() -> None:
    # Check that a config writable storage is return in get_writable_storage_keys()
    config_built_writable_storage_key = StorageKey.GENERIC_METRICS_SETS_RAW
    storage = get_storage(config_built_writable_storage_key)
    assert isinstance(storage, WritableTableStorage)
    assert config_built_writable_storage_key in list(get_config_built_storages().keys())
    assert config_built_writable_storage_key in get_writable_storage_keys()
