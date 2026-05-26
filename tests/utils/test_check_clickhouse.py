from __future__ import annotations

import importlib
import threading
from typing import Any, List, Sequence
from unittest import mock

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset
from snuba.datasets.readiness_state import ReadinessState
from snuba.datasets.storage import ReadableTableStorage, Storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.health_info import (
    check_all_tables_present,
    filter_checked_storages,
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
def test_non_essential_bad_storage_ignored(mock1: mock.MagicMock) -> None:
    # A non-essential broken storage (fake_storage_key) is filtered out before
    # the cluster check, so as long as the essential clusters are reachable,
    # the regular healthcheck still passes.
    assert sanity_check_clickhouse_connections()


@mock.patch(
    "snuba.utils.health_info.get_all_storage_keys",
    return_value=[FakeStorageKey("fake_storage_key")],
)
def test_no_essential_storages_fails(mock1: mock.MagicMock) -> None:
    # If no essential storages are enabled, the healthcheck fails defensively —
    # we have nothing to verify connectivity against.
    assert not sanity_check_clickhouse_connections()


@mock.patch(
    "snuba.utils.health_info.get_all_storage_keys",
    return_value=[StorageKey.ERRORS_RO],
)
@mock.patch("snuba.utils.health_info._execute_show_tables")
@pytest.mark.clickhouse_db
def test_unreachable_essential_cluster_fails_healthcheck(
    mock_execute: mock.MagicMock, mock_keys: mock.MagicMock
) -> None:
    # Per INC-2141: if an essential cluster is unreachable, the healthcheck must
    # fail (we can no longer short-circuit to True via another healthy cluster).
    mock_execute.side_effect = Exception("connection refused")
    assert not sanity_check_clickhouse_connections()


@mock.patch(
    "snuba.utils.health_info.get_all_storage_keys",
    return_value=[StorageKey.ERRORS_RO],
)
@mock.patch("snuba.utils.health_info._execute_show_tables")
@pytest.mark.clickhouse_db
def test_stalled_cluster_does_not_block_healthcheck(
    mock_execute: mock.MagicMock, mock_keys: mock.MagicMock
) -> None:
    # Per INC-2141: a stalled connection (e.g. EAP holding broken sockets) must
    # not keep sanity_check_clickhouse_connections from returning. The function
    # should respect its timeout and return False well before the stalled
    # worker finishes — not block until shutdown(wait=True) joins it.
    import time as _time

    stall_started = threading.Event()

    def stall(_cluster: object) -> bool:
        stall_started.set()
        _time.sleep(10)  # far longer than the timeout
        return True

    mock_execute.side_effect = stall

    timeout = 0.2
    start = _time.monotonic()
    result = sanity_check_clickhouse_connections(timeout_seconds=timeout)
    elapsed = _time.monotonic() - start

    assert stall_started.wait(timeout=2.0), "stalled worker never started"
    assert not result
    # Generous bound: timeout + executor teardown + test scheduler jitter, but
    # still well short of the 10s sleep. Without shutdown(wait=False) this
    # would wait ~10s.
    assert elapsed < 5.0, f"healthcheck blocked on stalled worker for {elapsed:.2f}s"


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
def test_dataset_undefined_storage_set(mock1: mock.MagicMock, mock2: mock.MagicMock) -> None:
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
