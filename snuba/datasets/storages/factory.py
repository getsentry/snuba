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
            self._cdc_storages: dict[StorageKey, Storage] = {}
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

        # TODO: Remove these as they are converted to configs
        from snuba.datasets.storages.discover import storage as discover_storage
        from snuba.datasets.storages.errors import storage as errors_storage
        from snuba.datasets.storages.errors_ro import storage as errors_ro_storage
        from snuba.datasets.storages.functions import (
            agg_storage as functions_ro_storage,
        )
        from snuba.datasets.storages.functions import raw_storage as functions_storage
        from snuba.datasets.storages.generic_metrics import (
            distributions_bucket_storage as gen_metrics_dists_bucket_storage,
        )
        from snuba.datasets.storages.generic_metrics import (
            distributions_storage as gen_metrics_dists_aggregate_storage,
        )
        from snuba.datasets.storages.generic_metrics import (
            sets_bucket_storage as gen_metrics_sets_bucket_storage,
        )
        from snuba.datasets.storages.generic_metrics import (
            sets_storage as gen_metrics_sets_aggregate_storage,
        )
        from snuba.datasets.storages.groupassignees import (
            storage as groupassignees_storage,
        )
        from snuba.datasets.storages.groupedmessages import (
            storage as groupedmessages_storage,
        )
        from snuba.datasets.storages.metrics import (
            counters_storage as metrics_counters_storage,
        )
        from snuba.datasets.storages.metrics import (
            distributions_storage as metrics_distributions_storage,
        )
        from snuba.datasets.storages.metrics import (
            org_counters_storage as metrics_org_counters_storage,
        )
        from snuba.datasets.storages.metrics import (
            polymorphic_bucket as metrics_polymorphic_storage,
        )
        from snuba.datasets.storages.metrics import sets_storage as metrics_sets_storage
        from snuba.datasets.storages.outcomes import (
            materialized_storage as outcomes_hourly_storage,
        )
        from snuba.datasets.storages.outcomes import raw_storage as outcomes_raw_storage
        from snuba.datasets.storages.profiles import (
            writable_storage as profiles_writable_storage,
        )
        from snuba.datasets.storages.querylog import storage as querylog_storage
        from snuba.datasets.storages.replays import storage as replays_storage
        from snuba.datasets.storages.sessions import (
            materialized_storage as sessions_hourly_storage,
        )
        from snuba.datasets.storages.sessions import (
            org_materialized_storage as org_sessions_hourly_storage,
        )
        from snuba.datasets.storages.sessions import raw_storage as sessions_raw_storage
        from snuba.datasets.storages.transactions import storage as transactions_storage

        self._cdc_storages = {
            storage.get_storage_key(): storage
            for storage in [groupedmessages_storage, groupassignees_storage]
        }

        self._all_storages = {
            **self._cdc_storages,
            **{
                storage.get_storage_key(): storage
                for storage in [
                    # WritableStorages
                    errors_storage,
                    outcomes_raw_storage,
                    querylog_storage,
                    sessions_raw_storage,
                    transactions_storage,
                    profiles_writable_storage,
                    functions_storage,
                    gen_metrics_sets_bucket_storage,
                    replays_storage,
                    gen_metrics_dists_bucket_storage,
                    metrics_distributions_storage,
                    metrics_sets_storage,
                    metrics_counters_storage,
                    metrics_polymorphic_storage,
                    # Readable Storages
                    discover_storage,
                    errors_ro_storage,
                    outcomes_hourly_storage,
                    sessions_hourly_storage,
                    org_sessions_hourly_storage,
                    profiles_writable_storage,
                    functions_ro_storage,
                    metrics_counters_storage,
                    metrics_distributions_storage,
                    metrics_org_counters_storage,
                    metrics_sets_storage,
                    gen_metrics_sets_aggregate_storage,
                    gen_metrics_dists_aggregate_storage,
                ]
            },
            **self._config_built_storages,
        }

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
