from snuba.clusters.storage_sets import StorageSetKey, is_valid_storage_set_combination


def test_storage_set_combination() -> None:
    assert (
        is_valid_storage_set_combination(StorageSetKey.EVENTS, StorageSetKey.CDC)
        is True
    )
    assert (
        is_valid_storage_set_combination(StorageSetKey.EVENTS, StorageSetKey.PROFILES)
        is False
    )
    assert (
        is_valid_storage_set_combination(
            StorageSetKey.EVENTS, StorageSetKey.CDC, StorageSetKey.PROFILES
        )
        is False
    )
