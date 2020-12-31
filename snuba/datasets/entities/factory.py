from typing import Callable, MutableMapping

from snuba.datasets.entities import EntityKey
from snuba.datasets.entity import Entity
from snuba.util import with_span


class InvalidEntityError(Exception):
    """Exception raised on invalid entity access."""


ENTITY_IMPL: MutableMapping[EntityKey, Entity] = {}
ENTITY_NAME_LOOKUP: MutableMapping[Entity, EntityKey] = {}


@with_span()
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
    from snuba.datasets.entities.errors import ErrorsEntity
    from snuba.datasets.entities.events import EventsEntity
    from snuba.datasets.entities.outcomes import OutcomesEntity
    from snuba.datasets.entities.outcomes_raw import OutcomesRawEntity
    from snuba.datasets.entities.sessions import SessionsEntity
    from snuba.datasets.entities.transactions import TransactionsEntity

    entity_factories: MutableMapping[EntityKey, Callable[[], Entity]] = {
        EntityKey.DISCOVER: DiscoverEntity,
        EntityKey.ERRORS: ErrorsEntity,
        EntityKey.EVENTS: EventsEntity,
        EntityKey.GROUPASSIGNEE: GroupAssigneeEntity,
        EntityKey.GROUPEDMESSAGES: GroupedMessageEntity,
        EntityKey.OUTCOMES: OutcomesEntity,
        EntityKey.OUTCOMES_RAW: OutcomesRawEntity,
        EntityKey.SESSIONS: SessionsEntity,
        EntityKey.TRANSACTIONS: TransactionsEntity,
        EntityKey.DISCOVER_TRANSACTIONS: DiscoverTransactionsEntity,
        EntityKey.DISCOVER_EVENTS: DiscoverEventsEntity,
    }

    try:
        entity = ENTITY_IMPL[name] = entity_factories[name]()
        ENTITY_NAME_LOOKUP[entity] = name
    except KeyError as error:
        raise InvalidEntityError(f"entity {name!r} does not exist") from error

    return entity
