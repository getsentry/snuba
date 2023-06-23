from typing import Iterator

import pytest

from snuba import settings
from snuba.datasets import factory
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import (
    InvalidDatasetError,
    get_dataset,
    get_dataset_name,
    get_enabled_dataset_names,
    reset_dataset_factory,
)


def test_get_dataset() -> None:
    for ds_name in [
        "discover",
        "events",
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


def test_get_dataset_factory_and_mapping_coupling() -> None:
    # Test will fail if the factory object initialization is decoupled from mapping initialization
    ds_name = "events"
    reset_dataset_factory()
    factory_ds = get_dataset(ds_name)
    assert isinstance(factory_ds, Dataset)
    assert get_dataset_name(factory_ds) == ds_name


def test_all_names() -> None:
    factory._DS_FACTORY = None
    assert set(get_enabled_dataset_names()) == set(
        [
            "discover",
            "events",
            "metrics",
            "outcomes",
            "outcomes_raw",
            "sessions",
            "transactions",
            "profiles",
            "functions",
            "generic_metrics",
            "replays",
            "search_issues",
            "spans",
        ]
    )
