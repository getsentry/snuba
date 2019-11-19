from typing import Callable, MutableMapping, Set, Sequence

from snuba import settings
from snuba.datasets.dataset import Dataset
from snuba.datasets.table_storage import TableWriter


DATASETS_IMPL: MutableMapping[str, Dataset] = {}

DATASET_NAMES: Set[str] = {
    "events",
    "groupassignee",
    "groupedmessage",
    "transactions",
    "outcomes",
    "outcomes_raw",
    "discover",
}


class InvalidDatasetError(Exception):
    """Exception raised on invalid dataset access."""


def get_dataset(name: str) -> Dataset:
    if name in DATASETS_IMPL:
        return DATASETS_IMPL[name]

    if name in settings.DISABLED_DATASETS:
        raise InvalidDatasetError(
            f"dataset {name!r} is not available in this environment"
        )

    from snuba.datasets.events import EventsDataset
    from snuba.datasets.cdc.groupassignee import GroupAssigneeDataset
    from snuba.datasets.cdc.groupedmessage import GroupedMessageDataset
    from snuba.datasets.transactions import TransactionsDataset
    from snuba.datasets.outcomes import OutcomesDataset
    from snuba.datasets.outcomes_raw import OutcomesRawDataset
    from snuba.datasets.groups import Groups
    from snuba.datasets.discover import DiscoverDataset

    dataset_factories: MutableMapping[str, Callable[[], Dataset]] = {
        "events": EventsDataset,
        "groupassignee": GroupAssigneeDataset,
        "groupedmessage": GroupedMessageDataset,
        "groups": Groups,
        "transactions": TransactionsDataset,
        "outcomes": OutcomesDataset,
        "outcomes_raw": OutcomesRawDataset,
        "discover": DiscoverDataset,
    }

    try:
        dataset = DATASETS_IMPL[name] = dataset_factories[name]()
    except KeyError as error:
        raise InvalidDatasetError(f"dataset {name!r} does not exist") from error

    return dataset


def get_enabled_dataset_names() -> Sequence[str]:
    return [name for name in DATASET_NAMES if name not in settings.DISABLED_DATASETS]


def enforce_table_writer(dataset: Dataset) -> TableWriter:
    table_writer = dataset.get_table_writer()
    assert table_writer is not None, f"Dataset{dataset} is not writable"
    return table_writer
