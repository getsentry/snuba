from typing import Generator, MutableMapping, Sequence, Type

from snuba import settings
from snuba.datasets.dataset import Dataset
from snuba.util import with_span
from snuba.utils.config_component_factory import ConfigComponentFactory
from snuba.utils.serializable_exception import SerializableException


class _DatasetFactory(ConfigComponentFactory[Dataset, str]):
    def __init__(self) -> None:
        self._dataset_map: MutableMapping[str, Dataset] = {}
        self._name_map: MutableMapping[Type[Dataset], str] = {}
        self.__initialize()

    def __initialize(self) -> None:
        from snuba.datasets.cdc.groupassignee import GroupAssigneeDataset
        from snuba.datasets.cdc.groupedmessage import GroupedMessageDataset
        from snuba.datasets.discover import DiscoverDataset
        from snuba.datasets.events import EventsDataset
        from snuba.datasets.functions import FunctionsDataset
        from snuba.datasets.generic_metrics import GenericMetricsDataset
        from snuba.datasets.metrics import MetricsDataset
        from snuba.datasets.outcomes import OutcomesDataset
        from snuba.datasets.outcomes_raw import OutcomesRawDataset
        from snuba.datasets.profiles import ProfilesDataset
        from snuba.datasets.replays import ReplaysDataset
        from snuba.datasets.sessions import SessionsDataset
        from snuba.datasets.transactions import TransactionsDataset

        self._dataset_map.update(
            {
                "discover": DiscoverDataset(),
                "events": EventsDataset(),
                "groupassignee": GroupAssigneeDataset(),
                "groupedmessage": GroupedMessageDataset(),
                "metrics": MetricsDataset(),
                "outcomes": OutcomesDataset(),
                "outcomes_raw": OutcomesRawDataset(),
                "sessions": SessionsDataset(),
                "transactions": TransactionsDataset(),
                "profiles": ProfilesDataset(),
                "functions": FunctionsDataset(),
                "generic_metrics": GenericMetricsDataset(),
                "replays": ReplaysDataset(),
            }
        )
        # TODO: load the yaml datasets here

        self._name_map = {v.__class__: k for k, v in self._dataset_map.items()}

    def iter_all(self) -> Generator[Dataset, None, None]:
        for dset in self._dataset_map.values():
            yield dset

    def all_names(self) -> Sequence[str]:
        return [
            name
            for name in self._dataset_map.keys()
            if name not in settings.DISABLED_DATASETS
        ]

    def get(self, name: str) -> Dataset:
        if name in settings.DISABLED_DATASETS:
            raise InvalidDatasetError(
                f"dataset {name!r} is disabled in this environment"
            )
        try:
            return self._dataset_map[name]
        except KeyError as error:
            raise InvalidDatasetError(f"dataset {name!r} does not exist") from error

    def get_dataset_name(self, dataset: Dataset) -> str:
        # TODO: This is dumb, the name should just be a property on the dataset
        try:
            return self._name_map[dataset.__class__]
        except KeyError as error:
            raise InvalidDatasetError(f"dataset {dataset} has no name") from error


class InvalidDatasetError(SerializableException):
    """Exception raised on invalid dataset access."""


_DS_FACTORY = None


def _ds_factory() -> _DatasetFactory:
    # This function can be acessed by many threads at once. It is okay if more than one thread recreates the same object.
    global _DS_FACTORY
    if _DS_FACTORY is None:
        _DS_FACTORY = _DatasetFactory()
    return _DS_FACTORY


@with_span()
def get_dataset(name: str) -> Dataset:
    return _ds_factory().get(name)


def get_dataset_name(dataset: Dataset) -> str:
    return _ds_factory().get_dataset_name(dataset)


def get_enabled_dataset_names() -> Sequence[str]:
    return _ds_factory().all_names()
