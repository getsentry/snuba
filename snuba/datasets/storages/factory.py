from __future__ import annotations

import logging
from glob import glob
from typing import Generator

import sentry_sdk

from snuba import settings
from snuba.datasets.cdc.cdcstorage import CdcStorage
from snuba.datasets.configuration.storage_builder import build_storage_from_config
from snuba.datasets.storage import ReadableTableStorage, Storage, WritableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.config_component_factory import ConfigComponentFactory

logger = logging.getLogger(__name__)

USE_CONFIG_BUILT_STORAGES = "use_config_built_storages"


class _StorageFactory(ConfigComponentFactory[Storage, StorageKey]):
    def __init__(self) -> None:
        with sentry_sdk.start_span(op="initialize", description="Storage Factory"):
            self._config_built_storages: dict[StorageKey, Storage] = {}
            self._all_storages: dict[StorageKey, Storage] = {}
            self.__initialize()

    def __initialize(self) -> None:
        self._config_built_storages = {
            storage.get_storage_key(): storage
            for storage in [
                build_storage_from_config(config_file)
                for config_file in glob(
                    settings.STORAGE_CONFIG_FILES_GLOB, recursive=True
                )
            ]
        }

        self._all_storages = self._config_built_storages

    def iter_all(self) -> Generator[Storage, None, None]:
        for storage in self._all_storages.values():
            yield storage

    def get(self, storage_key: StorageKey) -> Storage:
        return self._all_storages[storage_key]

    def get_writable_storage_keys(self) -> list[StorageKey]:
        return [
            storage_key
            for storage_key, storage in self._all_storages.items()
            if isinstance(storage, WritableTableStorage)
        ]

    def get_cdc_storage_keys(self) -> list[StorageKey]:
        return [
            storage_key
            for storage_key, storage in self._all_storages.items()
            if isinstance(storage, CdcStorage)
        ]

    def get_all_storage_keys(self) -> list[StorageKey]:
        return list(self._all_storages.keys())

    def get_config_built_storages(self) -> dict[StorageKey, Storage]:
        # TODO: Remove once all storages are config
        return self._config_built_storages


_STORAGE_FACTORY: _StorageFactory | None = None


def _storage_factory() -> _StorageFactory:
    global _STORAGE_FACTORY
    if _STORAGE_FACTORY is None:
        _STORAGE_FACTORY = _StorageFactory()
    return _STORAGE_FACTORY


def initialize_storage_factory() -> None:
    """
    Used to load storages on initialization of entities.
    """
    _storage_factory()


def get_storage(storage_key: StorageKey) -> ReadableTableStorage:
    storage = _storage_factory().get(storage_key)
    assert isinstance(storage, ReadableTableStorage)
    return storage


def get_writable_storage(storage_key: StorageKey) -> WritableTableStorage:
    storage = _storage_factory().get(storage_key)
    assert isinstance(storage, WritableTableStorage)
    return storage


def get_cdc_storage(storage_key: StorageKey) -> CdcStorage:
    storage = _storage_factory().get(storage_key)
    assert isinstance(storage, CdcStorage)
    return storage


def get_writable_storage_keys() -> list[StorageKey]:
    return _storage_factory().get_writable_storage_keys()


def get_cdc_storage_keys() -> list[StorageKey]:
    return _storage_factory().get_cdc_storage_keys()


def get_all_storage_keys() -> list[StorageKey]:
    return _storage_factory().get_all_storage_keys()


def get_config_built_storages() -> dict[StorageKey, Storage]:
    # TODO: Remove once all storages are config
    return _storage_factory().get_config_built_storages()
