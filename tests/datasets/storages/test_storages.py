import pytest

from snuba.datasets.storages import StorageKey, is_equivalent_write


@pytest.mark.parametrize(
    "first_storage, second_storage, expected",
    [
        (StorageKey.ERRORS, StorageKey.ERRORS_V2, True),
        (StorageKey.ERRORS, StorageKey.EVENTS, True),
        (StorageKey.TRANSACTIONS, StorageKey.TRANSACTIONS_V2, True),
        (StorageKey.ERRORS, StorageKey.TRANSACTIONS, False),
        (StorageKey.METRICS_SETS, StorageKey.METRICS_BUCKETS, False),
    ],
)
def test_storage_keys_same_writes(
    first_storage: StorageKey, second_storage: StorageKey, expected: bool
) -> None:
    assert is_equivalent_write(first_storage, second_storage) == expected
