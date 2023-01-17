import pytest

from snuba.datasets.storages.storage_key import StorageKey


def test_storage_key() -> None:
    with pytest.raises(AttributeError):
        StorageKey.NON_EXISTENT_STORAGE
