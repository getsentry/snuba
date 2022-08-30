import pytest

from snuba.datasets.storages import (
    CONFIG_BUILT_STORAGE_KEYS,
    StorageKey,
    are_writes_identical,
)


def test_storage_key() -> None:
    with pytest.raises(AttributeError):
        StorageKey.NON_EXISTENT_STORAGE

    assert CONFIG_BUILT_STORAGE_KEYS == {
        "GENERIC_METRICS_DISTRIBUTIONS": "generic_metrics_distributions",
        "GENERIC_METRICS_DISTRIBUTIONS_RAW": "generic_metrics_distributions_raw",
        "GENERIC_METRICS_SETS": "generic_metrics_sets",
        "GENERIC_METRICS_SETS_RAW": "generic_metrics_sets_raw",
    }


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
