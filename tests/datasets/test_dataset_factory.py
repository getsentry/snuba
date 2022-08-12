import pytest

from snuba import settings
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import (
    InvalidDatasetError,
    get_dataset,
    get_dataset_name,
    get_enabled_dataset_names,
)


def test_get_dataset():
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


@pytest.fixture(scope="function")
def disable_datasets():
    og_disabled = settings.DISABLED_DATASETS
    settings.DISABLED_DATASETS = ["events"]
    yield
    settings.DISABLED_DATASETS = og_disabled


def test_disabled(disable_datasets):
    assert "events" not in get_enabled_dataset_names()
    with pytest.raises(InvalidDatasetError):
        get_dataset("events")


def test_all_names():
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
