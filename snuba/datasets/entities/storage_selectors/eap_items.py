from collections.abc import Sequence
from typing import cast

import sentry_sdk

from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.storage import EntityStorageConnection
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.downsampled_storage_tiers import Tier
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.state.sentry_options import get_option

#: Sampling tiers served from the eap_items_2 tables rather than eap_items_1.
#: Listed per tier because eap_items_2's downsample tiers are populated after
#: its base table, so tier 1 has to be able to cut over on its own.
EAP_ITEMS_2_TIERS_OPTION = "eap_items_2_tiers"

_V1_PREFIX = "EAP_ITEMS"
_V2_PREFIX = "EAP_ITEMS_2"


def _storage_key_name(prefix: str, tier: Tier, readonly: bool) -> str:
    suffix = "_RO" if readonly else ""
    # TIER_NO_TIER (-1) means the caller never picked a tier, which is served
    # by the unsampled base table just like TIER_1.
    if tier in (Tier.TIER_1, Tier.TIER_NO_TIER):
        return f"{prefix}{suffix}"
    return f"{prefix}_DOWNSAMPLE_{tier.value}{suffix}"


def _use_eap_items_2(tier: Tier) -> bool:
    tiers = cast("list[int]", get_option(EAP_ITEMS_2_TIERS_OPTION, []))
    # TIER_NO_TIER is not nameable in the option; it follows tier 1.
    effective = Tier.TIER_1.value if tier == Tier.TIER_NO_TIER else tier.value
    return effective in tiers


def _registered_storage_key(name: str) -> StorageKey | None:
    """The StorageKey called ``name``, or None if no such storage is registered."""
    try:
        return cast(StorageKey, getattr(StorageKey, name))
    except AttributeError:
        return None


class EAPItemsStorageSelector(QueryStorageSelector):
    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_connections: Sequence[EntityStorageConnection],
    ) -> EntityStorageConnection:
        assert isinstance(query_settings, HTTPQuerySettings)

        tier = query_settings.get_sampling_tier()

        use_readonly_storage = (
            get_option("enable_eap_readonly_table", False) and not query_settings.get_consistent()
        )

        storage_key: StorageKey | None = None
        if _use_eap_items_2(tier):
            name = _storage_key_name(_V2_PREFIX, tier, use_readonly_storage)
            storage_key = _registered_storage_key(name)
            if storage_key is None:
                # The option named a tier whose eap_items_2 storage is not
                # registered in this deployment. Fall back to eap_items_1
                # rather than 500 mid-cutover: stale-but-correct data beats an
                # outage, and the message surfaces the misconfiguration.
                # RoutingStrategySelector handles a bad routing config the
                # same way.
                sentry_sdk.capture_message(
                    f"{EAP_ITEMS_2_TIERS_OPTION} selected {name}, which is not a registered "
                    f"storage; falling back to eap_items_1"
                )

        if storage_key is None:
            storage_key = getattr(
                StorageKey, _storage_key_name(_V1_PREFIX, tier, use_readonly_storage)
            )

        return EntityStorageConnection(
            storage=get_storage(storage_key),
            translation_mappers=storage_connections[0].translation_mappers,
        )
