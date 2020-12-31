from snuba.datasets.factory import get_dataset, get_dataset_name


def test_get_dataset_name() -> None:
    dataset_name = "events"
    assert get_dataset_name(get_dataset(dataset_name)) == dataset_name
