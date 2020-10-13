from enum import Enum
from typing import Callable, MutableMapping

from snuba.datasets.entity import Entity
from snuba.util import with_span


class InvalidEntityError(Exception):
    """Exception raised on invalid entity access."""


class EntityKey(Enum):
    """
    A entity key is a unique identifier for an entity.
    """

    DISCOVER = "discover"
    ERRORS = "errors"
    EVENTS = "events"
    GROUPS = "groups"
    GROUPASSIGNEE = "groupassignee"
    # TODO: This has an S on the end in solidarity with storages, but it's got to go
    GROUPEDMESSAGES = "groupedmessage"
    OUTCOMES = "outcomes"
    OUTCOMES_RAW = "outcomes_raw"
    SESSIONS = "sessions"
    TRANSACTIONS = "transactions"
    DISCOVER_TRANSACTIONS = "discover_transactions"


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
        DiscoverTransactionsEntity,
    )
    from snuba.datasets.entities.errors import ErrorsEntity
    from snuba.datasets.entities.events import EventsEntity
    from snuba.datasets.entities.groups import GroupsEntity
    from snuba.datasets.entities.outcomes import OutcomesEntity
    from snuba.datasets.entities.outcomes_raw import OutcomesRawEntity
    from snuba.datasets.entities.sessions import SessionsEntity
    from snuba.datasets.entities.transactions import TransactionsEntity

    entity_factories: MutableMapping[EntityKey, Callable[[], Entity]] = {
        EntityKey.DISCOVER: DiscoverEntity,
        EntityKey.ERRORS: ErrorsEntity,
        EntityKey.EVENTS: EventsEntity,
        EntityKey.GROUPS: GroupsEntity,
        EntityKey.GROUPASSIGNEE: GroupAssigneeEntity,
        EntityKey.GROUPEDMESSAGES: GroupedMessageEntity,
        EntityKey.OUTCOMES: OutcomesEntity,
        EntityKey.OUTCOMES_RAW: OutcomesRawEntity,
        EntityKey.SESSIONS: SessionsEntity,
        EntityKey.TRANSACTIONS: TransactionsEntity,
        EntityKey.DISCOVER_TRANSACTIONS: DiscoverTransactionsEntity,
    }

    try:
        entity = ENTITY_IMPL[name] = entity_factories[name]()
        ENTITY_NAME_LOOKUP[entity] = name
    except KeyError as error:
        raise InvalidEntityError(f"entity {name!r} does not exist") from error

    return entity
