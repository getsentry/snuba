import sys
from typing import Any

import pytest

from snuba.clusters.storage_sets import (
    _HARDCODED_STORAGE_SET_KEYS,
    _REGISTERED_STORAGE_SET_KEYS,
    StorageSetKey,
    is_valid_storage_set_combination,
)
from snuba.migrations.group_loader import GenericMetricsLoader


def test_storage_set_combination() -> None:
    assert is_valid_storage_set_combination(StorageSetKey.EVENTS, StorageSetKey.PROFILES) is False


def test_unregistered_storage_set_key_is_constructible() -> None:
    assert "GENERIC_METRICS_DISTRIBUTIONS" not in _HARDCODED_STORAGE_SET_KEYS
    popped = _REGISTERED_STORAGE_SET_KEYS.pop("GENERIC_METRICS_DISTRIBUTIONS", None)
    try:
        key = StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS
        assert key.value == "generic_metrics_distributions"
        assert key not in set(StorageSetKey)
    finally:
        if popped is not None:
            _REGISTERED_STORAGE_SET_KEYS["GENERIC_METRICS_DISTRIBUTIONS"] = popped


def test_private_storage_set_key_attr_raises() -> None:
    with pytest.raises(AttributeError):
        StorageSetKey._not_a_storage_set  # noqa: B018


def test_historical_migration_imports_without_cluster_registration() -> None:
    module = "snuba.snuba_migrations.generic_metrics.0007_distributions_aggregate_table"
    popped = _REGISTERED_STORAGE_SET_KEYS.pop("GENERIC_METRICS_DISTRIBUTIONS", None)
    sys.modules.pop(module, None)
    try:
        migration: Any = GenericMetricsLoader().load_migration("0007_distributions_aggregate_table")
        assert migration.storage_set_key.value == "generic_metrics_distributions"
    finally:
        sys.modules.pop(module, None)
        if popped is not None:
            _REGISTERED_STORAGE_SET_KEYS["GENERIC_METRICS_DISTRIBUTIONS"] = popped
