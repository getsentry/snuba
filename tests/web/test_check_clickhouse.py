from __future__ import annotations

import importlib
from typing import Any, Sequence
from unittest import mock

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset
from snuba.datasets.readiness_state import ReadinessState
from snuba.datasets.storage import ReadableTableStorage
from snuba.web.views import check_clickhouse, filter_checked_storages


class BadStorage(mock.MagicMock):
    def get_cluster(self) -> None:
        raise Exception("No cluster")


class BadEntity(mock.MagicMock):
    def get_all_storages(self) -> Sequence[BadStorage]:
        return [BadStorage()]


class ExperimentalDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(all_entities=[])

    @classmethod
    def is_experimental(cls) -> bool:
        return True

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

    @classmethod
    def is_experimental(cls) -> bool:
        return False

    def get_all_entities(self) -> Sequence[MockEntity]:
        return [MockEntity()]


class BadDataset(Dataset):
    def __init__(self) -> None:
        super().__init__(all_entities=[])

    @classmethod
    def is_experimental(cls) -> bool:
        return False

    def get_all_entities(self) -> Sequence[BadEntity]:
        return [BadEntity()]


def fake_get_dataset(name: str) -> Dataset:
    return {
        "events": get_dataset("events"),
        "experimental": ExperimentalDataset(),
        "bad": BadDataset(),
        "mock": MockDataset(),
    }[name]


@pytest.fixture(scope="function")
def temp_settings() -> Any:
    from snuba import settings

    yield settings
    importlib.reload(settings)


@mock.patch(
    "snuba.web.views.get_enabled_dataset_names",
    return_value=["events", "experimental"],
)
@mock.patch("snuba.web.views.get_dataset", side_effect=fake_get_dataset)
@pytest.mark.clickhouse_db
def test_check_clickhouse(mock1: mock.MagicMock, mock2: mock.MagicMock) -> None:
    assert check_clickhouse(ignore_experimental=True)
    assert not check_clickhouse(ignore_experimental=False)


@mock.patch(
    "snuba.web.views.get_enabled_dataset_names",
    return_value=["events", "bad"],
)
@mock.patch("snuba.web.views.get_dataset", side_effect=fake_get_dataset)
def test_bad_dataset_fails_healthcheck(
    mock1: mock.MagicMock, mock2: mock.MagicMock
) -> None:
    # the bad dataset is enabled and not experimental, therefore the healthcheck
    # should fail
    assert not check_clickhouse(ignore_experimental=True)


@mock.patch(
    "snuba.web.views.get_enabled_dataset_names",
    return_value=["events"],
)
@mock.patch("snuba.clusters.cluster._get_storage_set_cluster_map", return_value={})
def test_dataset_undefined_storage_set(
    mock1: mock.MagicMock, mock2: mock.MagicMock
) -> None:
    metrics_tags: dict[str, str] = {}
    assert not check_clickhouse(ignore_experimental=True, metric_tags=metrics_tags)
    for v in metrics_tags.values():
        assert isinstance(v, str)


@mock.patch(
    "snuba.web.views.get_enabled_dataset_names",
    return_value=["events", "experimental", "mock"],
)
@mock.patch("snuba.web.views.get_dataset", side_effect=fake_get_dataset)
@pytest.mark.clickhouse_db
def test_filter_checked_storages(
    mock1: mock.MagicMock, mock2: mock.MagicMock, temp_settings: Any
) -> None:
    temp_settings.SUPPORTED_STATES = {
        "limited",
        "partial",
        "complete",
    }  # remove deprecate from supported states
    temp_settings.READINESS_STATE_STORAGES_ENABLED = {"mock_storage"}
    storages = filter_checked_storages(ignore_experimental=True)

    # check experimental dataset's storage is not in list
    assert BadStorage() not in storages

    # checks errors storage is in list
    errors_storage = get_dataset("events").get_all_entities()[0].get_all_storages()[0]
    assert errors_storage in storages

    # check that the storage with a non-supported readiness state is excluded in list
    assert MockStorage() not in storages
