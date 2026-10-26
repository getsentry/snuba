import pytest
from sentry_options.testing import override_options

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
@override_options("snuba", {"enable_eap_readonly_table": True})
def test_selects_eap_items_ro_when_enabled() -> None:
    unimportant_query = Query(from_clause=EAP_ITEMS_ENTITY)
    query_settings = HTTPQuerySettings()
    query_settings.set_sampling_tier(Tier.TIER_1)

    selected_storage = EAPItemsStorageSelector().select_storage(
        unimportant_query, query_settings, EAP_ITEMS_STORAGE_CONNECTIONS
    )
    assert selected_storage.storage == get_storage(StorageKey.EAP_ITEMS_RO)


@pytest.mark.redis_db
@override_options("snuba", {"enable_eap_readonly_table": True})
def test_selects_writable_when_consistent() -> None:
    unimportant_query = Query(from_clause=EAP_ITEMS_ENTITY)
    query_settings = HTTPQuerySettings(consistent=True)
    query_settings.set_sampling_tier(Tier.TIER_1)

    selected_storage = EAPItemsStorageSelector().select_storage(
        unimportant_query, query_settings, EAP_ITEMS_STORAGE_CONNECTIONS
    )
    assert selected_storage.storage == get_storage(StorageKey.EAP_ITEMS)


@pytest.mark.redis_db
@override_options("snuba", {"enable_eap_readonly_table": True})
def test_selects_downsample_ro_when_enabled() -> None:
    unimportant_query = Query(from_clause=EAP_ITEMS_ENTITY)
    query_settings = HTTPQuerySettings()
    query_settings.set_sampling_tier(Tier.TIER_512)

    selected_storage = EAPItemsStorageSelector().select_storage(
        unimportant_query, query_settings, EAP_ITEMS_STORAGE_CONNECTIONS
    )
    assert selected_storage.storage == get_storage(StorageKey.EAP_ITEMS_DOWNSAMPLE_512_RO)


def _select(tier: Tier, consistent: bool = False) -> StorageKey:
    query_settings = HTTPQuerySettings(consistent=consistent)
    query_settings.set_sampling_tier(tier)
    return (
        EAPItemsStorageSelector()
        .select_storage(
            Query(from_clause=EAP_ITEMS_ENTITY), query_settings, EAP_ITEMS_STORAGE_CONNECTIONS
        )
        .storage.get_storage_key()
    )


ALL_TIERS = [Tier.TIER_NO_TIER, Tier.TIER_1, Tier.TIER_8, Tier.TIER_64, Tier.TIER_512]


@pytest.mark.redis_db
@pytest.mark.parametrize("tier", ALL_TIERS)
def test_eap_items_2_tiers_defaults_to_v1(tier: Tier) -> None:
    """With the option unset, nothing reaches the eap_items_2 storages."""
    assert "_2" not in _select(tier).value


@pytest.mark.redis_db
@override_options("snuba", {"eap_items_2_tiers": [1]})
def test_eap_items_2_tier_1_only_moves_the_base_table() -> None:
    """The downsample tiers must stay on v1 while only tier 1 is listed.

    eap_items_2's downsample partitions are attached after its base table, so
    moving them early would silently serve near-empty results.
    """
    assert _select(Tier.TIER_1) == StorageKey.EAP_ITEMS_2
    assert _select(Tier.TIER_NO_TIER) == StorageKey.EAP_ITEMS_2
    assert _select(Tier.TIER_8) == StorageKey.EAP_ITEMS_DOWNSAMPLE_8
    assert _select(Tier.TIER_64) == StorageKey.EAP_ITEMS_DOWNSAMPLE_64
    assert _select(Tier.TIER_512) == StorageKey.EAP_ITEMS_DOWNSAMPLE_512


@pytest.mark.redis_db
@override_options("snuba", {"eap_items_2_tiers": [1, 8, 64, 512]})
def test_eap_items_2_all_tiers() -> None:
    assert _select(Tier.TIER_1) == StorageKey.EAP_ITEMS_2
    assert _select(Tier.TIER_NO_TIER) == StorageKey.EAP_ITEMS_2
    assert _select(Tier.TIER_8) == StorageKey.EAP_ITEMS_2_DOWNSAMPLE_8
    assert _select(Tier.TIER_64) == StorageKey.EAP_ITEMS_2_DOWNSAMPLE_64
    assert _select(Tier.TIER_512) == StorageKey.EAP_ITEMS_2_DOWNSAMPLE_512


@pytest.mark.redis_db
@override_options(
    "snuba", {"eap_items_2_tiers": [1, 8, 64, 512], "enable_eap_readonly_table": True}
)
def test_eap_items_2_combines_with_readonly() -> None:
    assert _select(Tier.TIER_1) == StorageKey.EAP_ITEMS_2_RO
    assert _select(Tier.TIER_NO_TIER) == StorageKey.EAP_ITEMS_2_RO
    assert _select(Tier.TIER_8) == StorageKey.EAP_ITEMS_2_DOWNSAMPLE_8_RO
    assert _select(Tier.TIER_512) == StorageKey.EAP_ITEMS_2_DOWNSAMPLE_512_RO


@pytest.mark.redis_db
@override_options("snuba", {"eap_items_2_tiers": [1], "enable_eap_readonly_table": True})
def test_eap_items_2_consistent_query_uses_writable_v2() -> None:
    """A consistent query opts out of the read-only replica, not out of v2."""
    assert _select(Tier.TIER_1, consistent=True) == StorageKey.EAP_ITEMS_2


@pytest.mark.redis_db
@override_options("snuba", {"eap_items_2_tiers": [99]})
def test_eap_items_2_ignores_unrecognised_tiers() -> None:
    assert _select(Tier.TIER_1) == StorageKey.EAP_ITEMS
    assert _select(Tier.TIER_8) == StorageKey.EAP_ITEMS_DOWNSAMPLE_8


@pytest.mark.redis_db
@override_options("snuba", {"eap_items_2_tiers": [1]})
def test_eap_items_2_falls_back_to_v1_when_storage_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tier whose v2 storage is not registered must not 500 the query."""
    import snuba.datasets.entities.storage_selectors.eap_items as selector_module

    captured: list[str] = []
    monkeypatch.setattr(
        selector_module,
        "_registered_storage_key",
        lambda name: None if name.startswith("EAP_ITEMS_2") else StorageKey(name.lower()),
    )
    monkeypatch.setattr(selector_module.sentry_sdk, "capture_message", captured.append)

    assert _select(Tier.TIER_1) == StorageKey.EAP_ITEMS
    assert len(captured) == 1
    assert "EAP_ITEMS_2" in captured[0]
