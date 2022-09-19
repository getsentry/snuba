from typing import Sequence
from unittest import mock

from snuba.datasets.dataset import Dataset
from snuba.datasets.events import EventsDataset
from snuba.web.views import check_clickhouse


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
        "events": EventsDataset(),
        "experimental": ExperimentalDataset(),
        "bad": BadDataset(),
    }[name]


@mock.patch(
    "snuba.web.views.get_enabled_dataset_names",
    return_value=["events", "experimental"],
)
@mock.patch("snuba.web.views.get_dataset", side_effect=fake_get_dataset)
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
