import pytest

from snuba.clusters.storage_sets import (
    _HARDCODED_STORAGE_SET_KEYS,
    _REGISTERED_STORAGE_SET_KEYS,
    StorageSetKey,
    is_valid_storage_set_combination,
)


def test_storage_set_combination() -> None:
    assert is_valid_storage_set_combination(StorageSetKey.EVENTS, StorageSetKey.PROFILES) is False


def test_unregistered_storage_set_key_is_constructible() -> None:
    assert "UNREGISTERED_STORAGE_SET" not in _HARDCODED_STORAGE_SET_KEYS
    assert "UNREGISTERED_STORAGE_SET" not in _REGISTERED_STORAGE_SET_KEYS
    key = StorageSetKey.UNREGISTERED_STORAGE_SET
    assert key.value == "unregistered_storage_set"
    assert key not in set(StorageSetKey)


def test_private_storage_set_key_attr_raises() -> None:
    with pytest.raises(AttributeError):
        StorageSetKey._not_a_storage_set  # noqa: B018
