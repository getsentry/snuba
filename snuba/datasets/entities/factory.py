from __future__ import annotations

from glob import glob
from typing import Optional, Sequence, Type

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
            self._entity_map: dict[EntityKey, PluggableEntity] = {}
            self._name_map: dict[Type[Entity], EntityKey] = {}
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

        self._entity_map = {
            k: v
            for k, v in self._config_built_entities.items()
            if k.value not in settings.DISABLED_ENTITIES
        }
        self._name_map = {v.__class__: k for k, v in self._entity_map.items()}

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
def override_entity_map(name: EntityKey, entity: PluggableEntity) -> None:
    _ent_factory()._entity_map[name] = entity
