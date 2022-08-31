import pytest

from snuba.datasets.storages.storage_key import (
    REGISTERED_STORAGE_KEYS,
    KeyExistsError,
    StorageKey,
    are_writes_identical,
    register_storage_key,
)


def test_storage_key() -> None:
    assert REGISTERED_STORAGE_KEYS == {
        "GENERIC_METRICS_DISTRIBUTIONS": "generic_metrics_distributions",
        "GENERIC_METRICS_DISTRIBUTIONS_RAW": "generic_metrics_distributions_raw",
        "GENERIC_METRICS_SETS": "generic_metrics_sets",
        "GENERIC_METRICS_SETS_RAW": "generic_metrics_sets_raw",
    }
    sample_key = "sample_key"
    with pytest.raises(AttributeError):
        StorageKey.SAMPLE_KEY
    register_storage_key(sample_key)
    assert StorageKey.SAMPLE_KEY.value == sample_key
    with pytest.raises(KeyExistsError):
        register_storage_key(sample_key)


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
