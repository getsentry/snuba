from typing import Callable, MutableMapping

from snuba import settings
from snuba.datasets.entities import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.table_storage import TableWriter
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
    from snuba.datasets.entities.audit_log import AuditLogEntity
    from snuba.datasets.entities.discover import (
        DiscoverEntity,
        DiscoverEventsEntity,
        DiscoverTransactionsEntity,
    )
    from snuba.datasets.entities.events import EventsEntity
    from snuba.datasets.entities.functions import FunctionsEntity
    from snuba.datasets.entities.generic_metrics import (
        GenericMetricsDistributionsEntity,
        GenericMetricsSetsEntity,
    )
    from snuba.datasets.entities.metrics import (
        MetricsCountersEntity,
        MetricsDistributionsEntity,
        MetricsSetsEntity,
        OrgMetricsCountersEntity,
    )
    from snuba.datasets.entities.outcomes import OutcomesEntity
    from snuba.datasets.entities.outcomes_raw import OutcomesRawEntity
    from snuba.datasets.entities.profiles import ProfilesEntity
    from snuba.datasets.entities.replays import ReplaysEntity
    from snuba.datasets.entities.sessions import OrgSessionsEntity, SessionsEntity
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
        EntityKey.DISCOVER_TRANSACTIONS: DiscoverTransactionsEntity,
        EntityKey.DISCOVER_EVENTS: DiscoverEventsEntity,
        EntityKey.METRICS_SETS: MetricsSetsEntity,
        EntityKey.METRICS_COUNTERS: MetricsCountersEntity,
        EntityKey.ORG_METRICS_COUNTERS: OrgMetricsCountersEntity,
        EntityKey.METRICS_DISTRIBUTIONS: MetricsDistributionsEntity,
        EntityKey.PROFILES: ProfilesEntity,
        EntityKey.FUNCTIONS: FunctionsEntity,
        EntityKey.REPLAYS: ReplaysEntity,
        EntityKey.GENERIC_METRICS_SETS: GenericMetricsSetsEntity,
        EntityKey.GENERIC_METRICS_DISTRIBUTIONS: GenericMetricsDistributionsEntity,
        EntityKey.AUDIT_LOG: AuditLogEntity,
        **(dev_entity_factories if settings.ENABLE_DEV_FEATURES else {}),
    }

    try:
        entity = ENTITY_IMPL[name] = entity_factories[name]()
        ENTITY_NAME_LOOKUP[entity] = name
    except KeyError as error:
        raise InvalidEntityError(f"entity {name!r} does not exist") from error

    return entity


def enforce_table_writer(entity: Entity) -> TableWriter:
    writable_storage = entity.get_writable_storage()

    assert (
        writable_storage is not None
    ), f"Entity {ENTITY_NAME_LOOKUP[entity]} does not have a writable storage."
    return writable_storage.get_table_writer()
