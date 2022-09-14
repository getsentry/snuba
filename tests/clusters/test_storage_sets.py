from snuba.clusters.storage_set_key import StorageSetKey
from snuba.clusters.storage_sets import is_valid_storage_set_combination


def test_storage_set_combination() -> None:
    assert (
        is_valid_storage_set_combination(StorageSetKey.EVENTS, StorageSetKey.CDC)
        is True
    )
    assert (
        is_valid_storage_set_combination(StorageSetKey.EVENTS, StorageSetKey.SESSIONS)
        is False
    )
    assert (
        is_valid_storage_set_combination(
            StorageSetKey.EVENTS, StorageSetKey.CDC, StorageSetKey.SESSIONS
        )
        is False
    )
