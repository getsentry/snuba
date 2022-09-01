from typing import Generator, Mapping, MutableMapping, Optional, Sequence, Type

from snuba import settings
from snuba.datasets.configuration.entity_builder import build_entity_from_config
from snuba.datasets.entities import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.storages.factory import initialize_storage_factory
from snuba.datasets.table_storage import TableWriter
from snuba.utils.config_component_factory import ConfigComponentFactory
from snuba.utils.serializable_exception import SerializableException


class _EntityFactory(ConfigComponentFactory[Entity, EntityKey]):
    def __init__(self) -> None:
        initialize_storage_factory()
        self._entity_map: MutableMapping[EntityKey, Entity] = {}
        self._name_map: MutableMapping[Type[Entity], EntityKey] = {}
        self.__initialize()

    def __initialize(self) -> None:
        from snuba.datasets.cdc.groupassignee_entity import GroupAssigneeEntity
        from snuba.datasets.cdc.groupedmessage_entity import GroupedMessageEntity
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

        entity_to_config_path_mapping: Mapping[EntityKey, str] = {
            EntityKey.GENERIC_METRICS_SETS: "snuba/datasets/configuration/generic_metrics/entities/sets.yaml",
            EntityKey.GENERIC_METRICS_DISTRIBUTIONS: "snuba/datasets/configuration/generic_metrics/entities/distributions.yaml",
        }

        self._entity_map.update(
            {
                EntityKey.DISCOVER: DiscoverEntity(),
                EntityKey.EVENTS: EventsEntity(),
                EntityKey.GROUPASSIGNEE: GroupAssigneeEntity(),
                EntityKey.GROUPEDMESSAGES: GroupedMessageEntity(),
                EntityKey.OUTCOMES: OutcomesEntity(),
                EntityKey.OUTCOMES_RAW: OutcomesRawEntity(),
                EntityKey.SESSIONS: SessionsEntity(),
                EntityKey.ORG_SESSIONS: OrgSessionsEntity(),
                EntityKey.TRANSACTIONS: TransactionsEntity(),
                EntityKey.DISCOVER_TRANSACTIONS: DiscoverTransactionsEntity(),
                EntityKey.DISCOVER_EVENTS: DiscoverEventsEntity(),
                EntityKey.METRICS_SETS: MetricsSetsEntity(),
                EntityKey.METRICS_COUNTERS: MetricsCountersEntity(),
                EntityKey.ORG_METRICS_COUNTERS: OrgMetricsCountersEntity(),
                EntityKey.METRICS_DISTRIBUTIONS: MetricsDistributionsEntity(),
                EntityKey.PROFILES: ProfilesEntity(),
                EntityKey.FUNCTIONS: FunctionsEntity(),
                EntityKey.REPLAYS: ReplaysEntity(),
                EntityKey.GENERIC_METRICS_SETS: GenericMetricsSetsEntity(),
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS: GenericMetricsDistributionsEntity(),
            }
        )

        if settings.PREFER_PLUGGABLE_ENTITIES:
            self._entity_map.update(
                {
                    key: build_entity_from_config(path)
                    for (key, path) in entity_to_config_path_mapping.items()
                }
            )

        self._name_map = {v.__class__: k for k, v in self._entity_map.items()}

    def iter_all(self) -> Generator[Entity, None, None]:
        for ent in self._entity_map.values():
            yield ent

    def all_names(self) -> Sequence[EntityKey]:
        return [name for name in self._entity_map.keys()]

    def get(self, name: EntityKey) -> Entity:
        try:
            return self._entity_map[name]
        except KeyError as error:
            raise InvalidEntityError(f"entity {name!r} does not exist") from error

    def get_entity_name(self, entity: Entity) -> EntityKey:
        # TODO: This is dumb, the name should just be a property on the entity
        try:
            return self._name_map[entity.__class__]
        except KeyError as error:
            raise InvalidEntityError(f"entity {entity} has no name") from error


class InvalidEntityError(SerializableException):
    """Exception raised on invalid entity access."""


_ENT_FACTORY: Optional[_EntityFactory] = None


def _ent_factory() -> _EntityFactory:
    global _ENT_FACTORY
    if _ENT_FACTORY is None:
        _ENT_FACTORY = _EntityFactory()
    return _ENT_FACTORY


def get_entity(name: EntityKey) -> Entity:
    return _ent_factory().get(name)


def get_entity_name(entity: Entity) -> EntityKey:
    return _ent_factory().get_entity_name(entity)


def get_all_entity_names() -> Sequence[EntityKey]:
    return _ent_factory().all_names()


def enforce_table_writer(entity: Entity) -> TableWriter:
    writable_storage = entity.get_writable_storage()

    assert (
        writable_storage is not None
    ), f"Entity {_ent_factory().get_entity_name(entity)} does not have a writable storage."
    return writable_storage.get_table_writer()


def reset_entity_factory() -> None:
    global _ENT_FACTORY
    _ENT_FACTORY = _EntityFactory()


# Used by test cases to store FakeEntity. The reset_entity_factory() should be used after override.
def override_entity_map(name: EntityKey, entity: Entity) -> None:
    _ent_factory()._entity_map[name] = entity
