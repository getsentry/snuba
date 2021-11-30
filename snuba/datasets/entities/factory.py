from typing import Callable, MutableMapping

from snuba import settings
from snuba.datasets.entities import EntityKey
from snuba.datasets.entity import Entity
from snuba.utils.serializable_exception import SerializableException


class InvalidEntityError(SerializableException):
    """Exception raised on invalid entity access."""


ENTITY_IMPL: MutableMapping[EntityKey, Entity] = {}
ENTITY_NAME_LOOKUP: MutableMapping[Entity, EntityKey] = {}


def get_entity(name: EntityKey) -> Entity:
    if name in ENTITY_IMPL:
        return ENTITY_IMPL[name]

    from snuba.datasets.cdc.groupassignee_entity import GroupAssigneeEntity
    from snuba.datasets.cdc.groupedmessage_entity import GroupedMessageEntity
    from snuba.datasets.entities.discover import (
        DiscoverEntity,
        DiscoverEventsEntity,
        DiscoverTransactionsEntity,
    )
    from snuba.datasets.entities.events import EventsEntity
    from snuba.datasets.entities.metrics import (
        MetricsCountersEntity,
        MetricsDistributionsEntity,
        MetricsSetsEntity,
    )
    from snuba.datasets.entities.outcomes import OutcomesEntity
    from snuba.datasets.entities.outcomes_raw import OutcomesRawEntity
    from snuba.datasets.entities.sessions import OrgSessionsEntity, SessionsEntity
    from snuba.datasets.entities.spans import SpansEntity
    from snuba.datasets.entities.transactions import TransactionsEntity

    dev_entity_factories: MutableMapping[EntityKey, Callable[[], Entity]] = {}

    entity_factories: MutableMapping[EntityKey, Callable[[], Entity]] = {
        EntityKey.DISCOVER: DiscoverEntity,
        EntityKey.EVENTS: EventsEntity,
        EntityKey.GROUPASSIGNEE: GroupAssigneeEntity,
        EntityKey.GROUPEDMESSAGES: GroupedMessageEntity,
        EntityKey.OUTCOMES: OutcomesEntity,
        EntityKey.OUTCOMES_RAW: OutcomesRawEntity,
        EntityKey.SESSIONS: SessionsEntity,
        EntityKey.ORG_SESSIONS: OrgSessionsEntity,
        EntityKey.TRANSACTIONS: TransactionsEntity,
        EntityKey.SPANS: SpansEntity,
        EntityKey.DISCOVER_TRANSACTIONS: DiscoverTransactionsEntity,
        EntityKey.DISCOVER_EVENTS: DiscoverEventsEntity,
        EntityKey.METRICS_SETS: MetricsSetsEntity,
        EntityKey.METRICS_COUNTERS: MetricsCountersEntity,
        EntityKey.METRICS_DISTRIBUTIONS: MetricsDistributionsEntity,
        **(dev_entity_factories if settings.ENABLE_DEV_FEATURES else {}),
    }

    try:
        entity = ENTITY_IMPL[name] = entity_factories[name]()
        ENTITY_NAME_LOOKUP[entity] = name
    except KeyError as error:
        raise InvalidEntityError(f"entity {name!r} does not exist") from error

    return entity
