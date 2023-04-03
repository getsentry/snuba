from __future__ import annotations

from glob import glob
from typing import Type

import sentry_sdk

from snuba import settings
from snuba.datasets.configuration.dataset_builder import build_dataset_from_config
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import initialize_entity_factory
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.utils.config_component_factory import ConfigComponentFactory
from snuba.utils.metrics.util import with_span
from snuba.utils.serializable_exception import SerializableException


class _DatasetFactory(ConfigComponentFactory[Dataset, str]):
    def __init__(self) -> None:
        with sentry_sdk.start_span(op="initialize", description="Dataset Factory"):
            initialize_entity_factory()
            self._dataset_map: dict[str, Dataset] = {}
            self._name_map: dict[Type[Dataset], str] = {}
            self.__initialize()

    def __initialize(self) -> None:

        self._config_built_datasets: dict[str, Dataset] = {
            dataset.name: dataset
            for dataset in [
                build_dataset_from_config(config_file)
                for config_file in glob(
                    settings.DATASET_CONFIG_FILES_GLOB, recursive=True
                )
            ]
        }

        self._dataset_map.update(self._config_built_datasets)
        self._name_map = {v.__class__: k for k, v in self._dataset_map.items()}

    def all_names(self) -> list[str]:
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
        if isinstance(dataset, PluggableDataset):
            return dataset.name
        # TODO: Remove once all Datasets are generated from config (PluggableDatasets have name property)
        try:
            return self._name_map[dataset.__class__]
        except KeyError as error:
            raise InvalidDatasetError(f"dataset {dataset} has no name") from error

    def get_config_built_datasets(self) -> dict[str, Dataset]:
        return self._config_built_datasets


class InvalidDatasetError(SerializableException):
    """Exception raised on invalid dataset access."""


_DS_FACTORY: _DatasetFactory | None = None


def _ds_factory(reset: bool = False) -> _DatasetFactory:
    # This function can be acessed by many threads at once. It is okay if more than one thread recreates the same object.
    global _DS_FACTORY
    if _DS_FACTORY is None or reset:
        _DS_FACTORY = _DatasetFactory()
    return _DS_FACTORY


@with_span()
def get_dataset(name: str) -> Dataset:
    return _ds_factory().get(name)


def get_dataset_name(dataset: Dataset) -> str:
    return _ds_factory().get_dataset_name(dataset)


def get_enabled_dataset_names() -> list[str]:
    return _ds_factory().all_names()


def get_config_built_datasets() -> dict[str, Dataset]:
    # TODO: Remove once datasets are all config
    return _ds_factory().get_config_built_datasets()


def reset_dataset_factory() -> None:
    _ds_factory(reset=True)
