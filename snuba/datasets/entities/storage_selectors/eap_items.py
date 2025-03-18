from pathlib import Path
from typing import Sequence

from snuba.datasets.configuration.json_schema import STORAGE_VALIDATORS
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.configuration.storage_builder import (
    STORAGE_KEY,
    build_readable_storage_kwargs,
)
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.storage import EntityStorageConnection, ReadableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings


class EAPItemsStorageSelector(QueryStorageSelector):
    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_connections: Sequence[EntityStorageConnection],
    ) -> EntityStorageConnection:
        assert isinstance(query_settings, HTTPQuerySettings)
        tier = query_settings.get_tier()
        if tier == 1:
            return EntityStorageConnection(
                storage=get_storage(StorageKey.EAP_ITEMS),
                translation_mappers=storage_connections[0].translation_mappers,
            )

        else:
            eap_items_downsample_config = load_configuration_data(
                f"{Path(__file__).parent.parent.parent.as_posix()}/configuration/events_analytics_platform/storages/eap_items.yaml",
                STORAGE_VALIDATORS,
            )
            eap_items_downsample_storage_kwargs = build_readable_storage_kwargs(
                eap_items_downsample_config
            )
            eap_items_downsample_storage_kwargs[STORAGE_KEY] = StorageKey(
                f"EAP_ITEMS_DOWNSAMPLE_{tier}"
            )
            return EntityStorageConnection(
                storage=ReadableTableStorage(**eap_items_downsample_storage_kwargs),
                translation_mappers=storage_connections[0].translation_mappers,
            )
