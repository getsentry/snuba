from typing import Callable, MutableMapping, Set, Sequence

from snuba import settings
from snuba.datasets.dataset import Dataset
from snuba.datasets.table_storage import TableWriter
from snuba.util import with_span

DATASETS_IMPL: MutableMapping[str, Dataset] = {}
DATASETS_NAME_LOOKUP: MutableMapping[Dataset, str] = {}

DATASET_NAMES: Set[str] = {
    "discover",
    "events",
    "events_migration",
    "groupassignee",
    "groupedmessage",
    "outcomes",
    "outcomes_raw",
    "sessions",
    "transactions",
}

ACTIVE_DATASET_NAMES: Set[str] = DATASET_NAMES - settings.DISABLED_DATASETS


class InvalidDatasetError(Exception):
    """Exception raised on invalid dataset access."""


@with_span()
def get_dataset(name: str) -> Dataset:
    if name in DATASETS_IMPL:
        return DATASETS_IMPL[name]

    if name in settings.DISABLED_DATASETS:
        raise InvalidDatasetError(
            f"dataset {name!r} is not available in this environment"
        )

    from snuba.datasets.cdc.groupassignee import GroupAssigneeDataset
    from snuba.datasets.cdc.groupedmessage import GroupedMessageDataset
    from snuba.datasets.discover import DiscoverDataset
    from snuba.datasets.events import EventsDataset
    from snuba.datasets.errors import ErrorsDataset
    from snuba.datasets.outcomes import OutcomesDataset
    from snuba.datasets.outcomes_raw import OutcomesRawDataset
    from snuba.datasets.sessions import SessionsDataset
    from snuba.datasets.transactions import TransactionsDataset

    dataset_factories: MutableMapping[str, Callable[[], Dataset]] = {
        "discover": DiscoverDataset,
        "events": EventsDataset,
        "events_migration": ErrorsDataset,
        "groupassignee": GroupAssigneeDataset,
        "groupedmessage": GroupedMessageDataset,
        "outcomes": OutcomesDataset,
        "outcomes_raw": OutcomesRawDataset,
        "sessions": SessionsDataset,
        "transactions": TransactionsDataset,
    }

    try:
        dataset = DATASETS_IMPL[name] = dataset_factories[name]()
        DATASETS_NAME_LOOKUP[dataset] = name
    except KeyError as error:
        raise InvalidDatasetError(f"dataset {name!r} does not exist") from error

    return dataset


def get_dataset_name(dataset: Dataset) -> str:
    try:
        return DATASETS_NAME_LOOKUP[dataset]
    except KeyError as error:
        raise InvalidDatasetError("Dataset name not specified") from error


def get_enabled_dataset_names() -> Sequence[str]:
    return [name for name in DATASET_NAMES if name not in settings.DISABLED_DATASETS]


# TODO: This should be removed and moved to the Entity since Datasets no longer control
# storages.
def enforce_table_writer(dataset: Dataset) -> TableWriter:
    writable_storage = dataset.get_default_entity().get_writable_storage()

    assert (
        writable_storage is not None
    ), f"Dataset{dataset} does not have a writable storage."
    return writable_storage.get_table_writer()
