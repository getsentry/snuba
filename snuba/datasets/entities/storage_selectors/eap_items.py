from typing import Sequence

from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.storage import EntityStorageConnection
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.downsampled_storage_tiers import Tier
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

        tier = query_settings.get_sampling_tier()

        if tier == Tier.TIER_1 or tier == Tier.TIER_NO_TIER:
            storage_key = StorageKey.EAP_ITEMS
        else:
            storage_key = getattr(StorageKey, f"EAP_ITEMS_DOWNSAMPLE_{tier.value}")

        return EntityStorageConnection(
            storage=get_storage(storage_key),
            translation_mappers=storage_connections[0].translation_mappers,
        )
