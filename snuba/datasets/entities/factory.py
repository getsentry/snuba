import functools
from typing import cast

from snuba.datasets.entities import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.table_storage import TableWriter
from snuba.utils.serializable_exception import SerializableException


class InvalidEntityError(SerializableException):
    """Exception raised on invalid entity access."""


class _EntityNameLookuper:
    def __getitem__(self, key: Entity) -> EntityKey:
        registry_key = key.registry_key()
        entities = [e for e in EntityKey if e.value == registry_key]
        if entities:
            return entities[0]
        raise KeyError(key)


# for some reason subscriptions gets the EntityKey from the Entity class so we have
# to do this for now
ENTITY_NAME_LOOKUP = _EntityNameLookuper()


def import_entities() -> None:
    from snuba.datasets.cdc.groupassignee_entity import (  # noqa: F401
        GroupAssigneeEntity,
    )
    from snuba.datasets.cdc.groupedmessage_entity import (  # noqa: F401
        GroupedMessageEntity,
    )
    from snuba.datasets.entities.discover import (  # noqa: F401
        DiscoverEntity,
        DiscoverEventsEntity,
        DiscoverTransactionsEntity,
    )
    from snuba.datasets.entities.events import EventsEntity  # noqa: F401
    from snuba.datasets.entities.metrics import (  # noqa: F401
        MetricsCountersEntity,
        MetricsDistributionsEntity,
        MetricsSetsEntity,
        OrgMetricsCountersEntity,
    )
    from snuba.datasets.entities.outcomes import OutcomesEntity  # noqa: F401
    from snuba.datasets.entities.outcomes_raw import OutcomesRawEntity  # noqa: F401
    from snuba.datasets.entities.profiles import ProfilesEntity  # noqa: F401
    from snuba.datasets.entities.sessions import (  # noqa: F401
        OrgSessionsEntity,
        SessionsEntity,
    )
    from snuba.datasets.entities.transactions import TransactionsEntity  # noqa: F401


@functools.lru_cache(maxsize=420)
def get_entity(name: EntityKey) -> Entity:
    registered_entity = Entity.from_name(name.value)
    if registered_entity is not None:
        return cast(Entity, registered_entity())
    import_entities()
    registered_entity = Entity.from_name(name.value)
    if registered_entity is not None:
        return cast(Entity, registered_entity())

    raise InvalidEntityError(f"entity {name!r} does not exist")


def enforce_table_writer(entity: Entity) -> TableWriter:
    writable_storage = entity.get_writable_storage()

    assert (
        writable_storage is not None
    ), f"Entity {entity.__class__.__name__} does not have a writable storage."
    return writable_storage.get_table_writer()
