import pytest

from snuba.clusters.cluster import get_cluster
from snuba.datasets.factory import (
    get_dataset,
    get_dataset_name,
    get_enabled_dataset_names,
)


def test_get_dataset_name() -> None:
    dataset_name = "events"
    assert get_dataset_name(get_dataset(dataset_name)) == dataset_name


DATASETS = [pytest.param(name, id=name) for name in get_enabled_dataset_names()]


@pytest.mark.parametrize("dataset_name", DATASETS)
def test_dataset_load(dataset_name: str) -> None:
    """
    Tests that if we decalare a dataset that is not in in dev. It can
    be fully loaded including all its entities and storages
    """

    dataset = get_dataset(dataset_name)
    for entity in dataset.get_all_entities():
        for storage in entity.get_all_storages():
            get_cluster(storage.get_storage_set_key())
