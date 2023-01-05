from glob import glob
from typing import Generator, MutableMapping, Optional, Sequence, Type

import sentry_sdk

from snuba import settings
from snuba.datasets.configuration.entity_builder import build_entity_from_config
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.datasets.storages.factory import initialize_storage_factory
from snuba.datasets.table_storage import TableWriter
from snuba.utils.config_component_factory import ConfigComponentFactory
from snuba.utils.serializable_exception import SerializableException


class _EntityFactory(ConfigComponentFactory[Entity, EntityKey]):
    def __init__(self) -> None:
        with sentry_sdk.start_span(op="initialize", description="Entity Factory"):
            initialize_storage_factory()
            self._entity_map: MutableMapping[EntityKey, Entity] = {}
            self._name_map: MutableMapping[Type[Entity], EntityKey] = {}
            self.__initialize()

    def __initialize(self) -> None:

        self._config_built_entities = {
            entity.entity_key: entity
            for entity in [
                build_entity_from_config(config_file)
                for config_file in glob(
                    settings.ENTITY_CONFIG_FILES_GLOB, recursive=True
                )
            ]
        }

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

        entity_map_pre_execute = {
            EntityKey.DISCOVER: DiscoverEntity,
            EntityKey.EVENTS: EventsEntity,
            EntityKey.GROUPASSIGNEE: GroupAssigneeEntity,
            EntityKey.GROUPEDMESSAGE: GroupedMessageEntity,
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
        }

        self._entity_map.update(
            {
                k: v()
                for (k, v) in entity_map_pre_execute.items()
                if k.value not in settings.DISABLED_ENTITIES
            }
        )

        self._entity_map.update(self._config_built_entities)

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
        try:
            if isinstance(entity, PluggableEntity):
                return entity.entity_key
            # TODO: Destroy all non-PluggableEntity Entities
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


def initialize_entity_factory() -> None:
    """
    Used to load entities on initialization of datasets.
    """
    _ent_factory()


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
