import pytest

from snuba.datasets.storages.storage_key import StorageKey, are_writes_identical


def test_storage_key() -> None:
    with pytest.raises(AttributeError):
        StorageKey.NON_EXISTENT_STORAGE


@pytest.mark.parametrize(
    "first_storage, second_storage, expected",
    [
        (StorageKey.ERRORS, StorageKey.ERRORS_V2, True),
        (StorageKey.TRANSACTIONS, StorageKey.TRANSACTIONS_V2, True),
        (StorageKey.ERRORS, StorageKey.TRANSACTIONS, False),
    ],
)
def test_storage_keys_same_writes(
    first_storage: StorageKey, second_storage: StorageKey, expected: bool
) -> None:
    assert are_writes_identical(first_storage, second_storage) == expected
