from snuba.clusters.storage_sets import StorageSetKey, is_valid_storage_set_combination


def test_storage_set_combination() -> None:
    assert is_valid_storage_set_combination(StorageSetKey.EVENTS, StorageSetKey.PROFILES) is False
