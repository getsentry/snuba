import threading
from typing import Any, Iterator

import pytest

from snuba import settings
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import (
    InvalidDatasetError,
    get_dataset,
    get_dataset_name,
    get_enabled_dataset_names,
)


def test_get_dataset() -> None:
    for ds_name in [
        "discover",
        "events",
        "groupassignee",
        "groupedmessage",
        "metrics",
        "outcomes",
        "outcomes_raw",
        "sessions",
        "transactions",
        "profiles",
        "functions",
        "generic_metrics",
        "replays",
    ]:
        factory_ds = get_dataset(ds_name)
        assert isinstance(factory_ds, Dataset)
        assert get_dataset_name(factory_ds) == ds_name


def test_get_dataset_multithreaded_collision() -> None:
    class GetDatasetThread(threading.Thread):
        def test_get_dataset_threaded(self) -> None:
            ds_name = "events"
            factory_ds = get_dataset(ds_name)
            assert isinstance(factory_ds, Dataset)
            assert get_dataset_name(factory_ds) == ds_name

        def run(self) -> None:
            self.exception = None
            try:
                self.test_get_dataset_threaded()
            except Exception as e:
                self.exception = e

        def join(self, *args: Any, **kwargs: Any) -> None:
            threading.Thread.join(self)
            if self.exception:
                raise self.exception

    threads = []
    for _ in range(10):
        thread = GetDatasetThread()
        threads.append(thread)
        thread.start()

    for thread in threads:
        try:
            thread.join()
        except Exception as error:
            raise error


@pytest.fixture(scope="function")
def disable_datasets() -> Iterator[None]:
    og_disabled = settings.DISABLED_DATASETS
    settings.DISABLED_DATASETS = set(["events"])
    yield
    settings.DISABLED_DATASETS = og_disabled


def test_disabled(disable_datasets: Iterator[None]) -> None:
    assert "events" not in get_enabled_dataset_names()
    with pytest.raises(InvalidDatasetError):
        get_dataset("events")


def test_all_names() -> None:
    assert set(get_enabled_dataset_names()) == set(
        [
            "discover",
            "events",
            "groupassignee",
            "groupedmessage",
            "metrics",
            "outcomes",
            "outcomes_raw",
            "sessions",
            "transactions",
            "profiles",
            "functions",
            "generic_metrics",
            "replays",
        ]
    )
