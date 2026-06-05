import pytest

from snuba import state
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entities.storage_selectors.eap_items import EAPItemsStorageSelector
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.downsampled_storage_tiers import Tier
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings

EAP_ITEMS_ENTITY = Entity(
    key=EntityKey("eap_items"),
    schema=get_entity(EntityKey("eap_items")).get_data_model(),
    sample=None,
)

EAP_ITEMS_STORAGE_CONNECTIONS = get_entity(EntityKey.EAP_ITEMS).get_all_storage_connections()


def test_selects_eap_items() -> None:
    unimportant_query = Query(from_clause=EAP_ITEMS_ENTITY)
    query_settings = HTTPQuerySettings()
    query_settings.set_sampling_tier(Tier.TIER_1)

    selected_storage = EAPItemsStorageSelector().select_storage(
        unimportant_query, query_settings, EAP_ITEMS_STORAGE_CONNECTIONS
    )
    assert selected_storage.storage == get_storage(StorageKey.EAP_ITEMS)


def test_selects_correct_eap_items_tier() -> None:
    unimportant_query = Query(from_clause=EAP_ITEMS_ENTITY)
    query_settings = HTTPQuerySettings()
    query_settings.set_sampling_tier(Tier.TIER_512)

    selected_storage = EAPItemsStorageSelector().select_storage(
        unimportant_query, query_settings, EAP_ITEMS_STORAGE_CONNECTIONS
    )
    assert selected_storage.storage == get_storage(StorageKey.EAP_ITEMS_DOWNSAMPLE_512)


@pytest.mark.redis_db
def test_selects_eap_items_ro_when_enabled() -> None:
    unimportant_query = Query(from_clause=EAP_ITEMS_ENTITY)
    query_settings = HTTPQuerySettings()
    query_settings.set_sampling_tier(Tier.TIER_1)

    state.set_config("enable_eap_readonly_table", 1)
    try:
        selected_storage = EAPItemsStorageSelector().select_storage(
            unimportant_query, query_settings, EAP_ITEMS_STORAGE_CONNECTIONS
        )
        assert selected_storage.storage == get_storage(StorageKey.EAP_ITEMS_RO)
    finally:
        state.delete_config("enable_eap_readonly_table")


@pytest.mark.redis_db
def test_selects_writable_when_consistent() -> None:
    unimportant_query = Query(from_clause=EAP_ITEMS_ENTITY)
    query_settings = HTTPQuerySettings(consistent=True)
    query_settings.set_sampling_tier(Tier.TIER_1)

    state.set_config("enable_eap_readonly_table", 1)
    try:
        selected_storage = EAPItemsStorageSelector().select_storage(
            unimportant_query, query_settings, EAP_ITEMS_STORAGE_CONNECTIONS
        )
        assert selected_storage.storage == get_storage(StorageKey.EAP_ITEMS)
    finally:
        state.delete_config("enable_eap_readonly_table")


@pytest.mark.redis_db
def test_selects_downsample_ro_when_enabled() -> None:
    unimportant_query = Query(from_clause=EAP_ITEMS_ENTITY)
    query_settings = HTTPQuerySettings()
    query_settings.set_sampling_tier(Tier.TIER_512)

    state.set_config("enable_eap_readonly_table", 1)
    try:
        selected_storage = EAPItemsStorageSelector().select_storage(
            unimportant_query, query_settings, EAP_ITEMS_STORAGE_CONNECTIONS
        )
        assert selected_storage.storage == get_storage(StorageKey.EAP_ITEMS_DOWNSAMPLE_512_RO)
    finally:
        state.delete_config("enable_eap_readonly_table")


@pytest.mark.redis_db
def test_forced_downsample_killswtich() -> None:
    unimportant_query = Query(from_clause=EAP_ITEMS_ENTITY)
    query_settings = HTTPQuerySettings()
    query_settings.set_sampling_tier(Tier.TIER_1)

    state.set_config("forced_downsample_killswitch", 1)
    try:
        selected_storage = EAPItemsStorageSelector().select_storage(
            unimportant_query, query_settings, EAP_ITEMS_STORAGE_CONNECTIONS
        )
        assert selected_storage.storage == get_storage(StorageKey.EAP_ITEMS_DOWNSAMPLE_8)
    finally:
        state.delete_config("forced_downsample_killswitch")
