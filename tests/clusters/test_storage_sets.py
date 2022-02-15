import pytest

from snuba.clusters.storage_sets import StorageSetKey, is_valid_storage_set_combination


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


@pytest.mark.parametrize(
    "storage,expected",
    [("events", True), ("transactions", True), ("non-existent-storage", False)],
)
def test_storage_set_to_set_method(storage: str, expected: bool) -> None:
    assert (storage in StorageSetKey.to_set()) is expected
