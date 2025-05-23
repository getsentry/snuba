from snuba.datasets.entities import factory
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import (
    get_all_entity_names,
    get_entity,
    get_entity_name,
    initialize_entity_factory,
    reset_entity_factory,
)
from snuba.datasets.entity import Entity

initialize_entity_factory()

ENTITY_KEYS = [
    EntityKey.DISCOVER,
    EntityKey.EVENTS,
    EntityKey.SPANS_NUM_ATTRS,
    EntityKey.SPANS_STR_ATTRS,
    EntityKey.GROUPASSIGNEE,
    EntityKey.GROUPEDMESSAGE,
    EntityKey.OUTCOMES,
    EntityKey.OUTCOMES_RAW,
    EntityKey.SEARCH_ISSUES,
    EntityKey.TRANSACTIONS,
    EntityKey.DISCOVER_TRANSACTIONS,
    EntityKey.DISCOVER_EVENTS,
    EntityKey.METRICS_SETS,
    EntityKey.METRICS_COUNTERS,
    EntityKey.ORG_METRICS_COUNTERS,
    EntityKey.METRICS_DISTRIBUTIONS,
    EntityKey.PROFILES,
    EntityKey.FUNCTIONS,
    EntityKey.REPLAYS,
    EntityKey.REPLAYS_AGGREGATED,
    EntityKey.GENERIC_METRICS_SETS,
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    EntityKey.GENERIC_METRICS_COUNTERS,
    EntityKey.GENERIC_ORG_METRICS_COUNTERS,
    EntityKey.GENERIC_METRICS_GAUGES,
    EntityKey.SPANS,
    EntityKey.GROUP_ATTRIBUTES,
    EntityKey.__INTERNAL_GENERIC_ORG_METRICS_DISTRIBUTIONS,
    EntityKey.__INTERNAL_GENERIC_ORG_METRICS_SETS,
    EntityKey.__INTERNAL_ORG_METRICS_DISTRIBUTIONS,
    EntityKey.__INTERNAL_ORG_METRICS_SETS,
    EntityKey.GENERIC_METRICS_GAUGES_META,
    EntityKey.GENERIC_METRICS_GAUGES_META_TAG_VALUES,
    EntityKey.GENERIC_METRICS_SETS_META,
    EntityKey.GENERIC_METRICS_SETS_META_TAG_VALUES,
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS_META,
    EntityKey.GENERIC_METRICS_COUNTERS_META,
    EntityKey.GENERIC_METRICS_COUNTERS_META_TAG_VALUES,
    EntityKey.UPTIME_CHECKS,
    EntityKey.EAP_ITEMS,
    EntityKey.EAP_ITEMS_SPAN,
]


def test_get_entity() -> None:
    for ent_name in ENTITY_KEYS:
        entity = get_entity(ent_name)
        assert isinstance(entity, Entity)
        assert get_entity_name(entity) == ent_name


def test_get_entity_factory_and_mapping_coupling() -> None:
    # Test will fail if the factory object initialization is decoupled from mapping initialization
    ent_name = EntityKey.EVENTS
    reset_entity_factory()
    entity = get_entity(ent_name)
    assert isinstance(entity, Entity)
    assert get_entity_name(entity) == ent_name


def test_all_names() -> None:
    factory._ENT_FACTORY = None
    assert set(get_all_entity_names()) == set(ENTITY_KEYS)
