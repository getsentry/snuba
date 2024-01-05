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
    EntityKey.GROUPASSIGNEE,
    EntityKey.GROUPEDMESSAGE,
    EntityKey.OUTCOMES,
    EntityKey.OUTCOMES_RAW,
    EntityKey.SESSIONS,
    EntityKey.SEARCH_ISSUES,
    EntityKey.ORG_SESSIONS,
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
    EntityKey.GENERIC_METRICS_SETS,
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    EntityKey.GENERIC_METRICS_COUNTERS,
    EntityKey.GENERIC_ORG_METRICS_COUNTERS,
    EntityKey.GENERIC_METRICS_GAUGES,
    EntityKey.SPANS,
    EntityKey.GROUP_ATTRIBUTES,
    EntityKey.METRICS_SUMMARIES,
    EntityKey.__INTERNAL_GENERIC_ORG_METRICS_DISTRIBUTIONS,
    EntityKey.__INTERNAL_GENERIC_ORG_METRICS_SETS,
    EntityKey.__INTERNAL_ORG_METRICS_DISTRIBUTIONS,
    EntityKey.__INTERNAL_ORG_METRICS_SETS,
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
