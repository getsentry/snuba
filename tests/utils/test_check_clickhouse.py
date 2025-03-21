from __future__ import annotations

import importlib
from typing import Any, List, Sequence
from unittest import mock

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset
from snuba.datasets.readiness_state import ReadinessState
from snuba.datasets.storage import ReadableTableStorage, Storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.health_info import (
    _set_shutdown,
    check_all_tables_present,
    filter_checked_storages,
    get_shutdown,
    sanity_check_clickhouse_connections,
)


class BadStorage(mock.MagicMock):
    def get_readiness_state(self) -> ReadinessState:
        return ReadinessState.DEPRECATE

    def get_cluster(self) -> None:
        raise Exception("No cluster")


class BadEntity(mock.MagicMock):
    def get_all_storages(self) -> Sequence[BadStorage]:
        return [BadStorage()]


class ExperimentalDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(all_entities=[])

    def get_all_entities(self) -> Sequence[BadEntity]:
        return [BadEntity()]


class MockStorage(mock.MagicMock):
    def get_cluster(self) -> None:
        raise Exception("No cluster")

    def get_readiness_state(self) -> ReadinessState:
        return ReadinessState.DEPRECATE

    def get_storage_key(self) -> Any:
        class TempStorageKey(object):
            value = "mock_storage"

        storage_key = TempStorageKey()
        return storage_key


class MockEntity(mock.MagicMock):
    def get_all_storages(self) -> Sequence[MockStorage]:
        return [MockStorage(spec=ReadableTableStorage)]


class MockDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(all_entities=[])

    def get_all_entities(self) -> Sequence[MockEntity]:
        return [MockEntity()]


class BadDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(all_entities=[])

    def get_all_entities(self) -> Sequence[BadEntity]:
        return [BadEntity()]


def fake_get_dataset(name: str) -> Dataset:
    return {
        "events": get_dataset("events"),
        "experimental": ExperimentalDataset(),
        "bad": BadDataset(),
        "mock": MockDataset(),
    }[name]


class FakeStorageKey(StorageKey):
    def __init__(self, value: str) -> None:
        super().__init__(value=value)


def fake_get_all_storage_keys() -> list[StorageKey]:
    return [FakeStorageKey("fake_storage_key")]


@pytest.fixture(scope="function")
def temp_settings() -> Any:
    from snuba import settings

    yield settings
    importlib.reload(settings)


@mock.patch(
    "snuba.utils.health_info.get_all_storage_keys",
    return_value=[StorageKey.ERRORS_RO],
)
@pytest.mark.clickhouse_db
def test_check_all_tables_present(mock1: mock.MagicMock) -> None:
    assert check_all_tables_present()


@mock.patch(
    "snuba.utils.health_info.get_all_storage_keys",
    return_value=[StorageKey.ERRORS_RO],
)
@pytest.mark.clickhouse_db
def test_sanity_check_clickhouse_connections(mock1: mock.MagicMock) -> None:
    assert sanity_check_clickhouse_connections()


@mock.patch(
    "snuba.utils.health_info.get_all_storage_keys",
    return_value=[StorageKey.ERRORS_RO, FakeStorageKey("fake_storage_key")],
)
def test_bad_dataset_fails_thorough_healthcheck(mock1: mock.MagicMock) -> None:
    # the bad dataset is enabled and not experimental, therefore the healthcheck
    # should fail
    assert not check_all_tables_present()


@mock.patch(
    "snuba.utils.health_info.get_all_storage_keys",
    return_value=[StorageKey.ERRORS_RO, FakeStorageKey("fake_storage_key")],
)
@pytest.mark.clickhouse_db
def test_single_bad_dataset_passes_healthcheck(mock1: mock.MagicMock) -> None:
    # a bad dataset is enabled and not operable, but at least a single
    # clickhouse query node is still operable. We still want to pass the
    # regular healthcheck in this case so that we can continue to serve request
    # despite outage in a single cluster.
    assert sanity_check_clickhouse_connections()


@mock.patch(
    "snuba.utils.health_info.get_all_storage_keys",
    return_value=[FakeStorageKey("fake_storage_key")],
)
def test_all_bad_dataset_fails_thorough_healthcheck(mock1: mock.MagicMock) -> None:
    # No query nodes are available, fail healthcheck because there's likely a
    # connection issue with the underlying host
    assert not check_all_tables_present()


@mock.patch(
    "snuba.utils.health_info.get_all_storage_keys",
    return_value=[StorageKey.ERRORS_RO],
)
@mock.patch("snuba.clusters.cluster._get_storage_set_cluster_map", return_value={})
def test_dataset_undefined_storage_set(
    mock1: mock.MagicMock, mock2: mock.MagicMock
) -> None:
    metrics_tags: dict[str, str] = {}
    assert not check_all_tables_present(metric_tags=metrics_tags)
    for v in metrics_tags.values():
        assert isinstance(v, str)


@mock.patch(
    "snuba.utils.health_info.get_all_storage_keys",
    return_value=[StorageKey.ERRORS_RO],
)
@pytest.mark.clickhouse_db
def test_filter_checked_storages(mock1: mock.MagicMock, temp_settings: Any) -> None:
    temp_settings.SUPPORTED_STATES = {
        "limited",
        "partial",
        "complete",
    }  # remove deprecate from supported states
    storages: List[Storage] = []
    filter_checked_storages(storages)

    # check experimental dataset's storage is not in list
    assert BadStorage() not in storages

    # checks errors storage is in list
    errors_storage = get_dataset("events").get_all_entities()[0].get_all_storages()[0]
    assert errors_storage in storages

    # check that the storage with a non-supported readiness state is excluded in list
    assert MockStorage() not in storages


def test_get_shutdown() -> None:
    assert not get_shutdown()
    _set_shutdown(True)
    assert get_shutdown()

    _set_shutdown(False)
    assert not get_shutdown()
