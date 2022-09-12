from __future__ import annotations

from typing import Any

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity


class PluggableDataset(Dataset):
    """
    PluggableDataset is a version of Dataset that is designed to be populated by
    static YAML-based configuration files. It is intentionally less flexible
    than Dataset. See the documentation of Dataset for explanation about how
    overridden methods are supposed to behave.
    """

    def __init__(
        self,
        *,
        name: str,
        default_entity: EntityKey,
        all_entities: list[EntityKey] | None,
        is_experimental: bool | None,
    ) -> None:
        super().__init__(default_entity=default_entity)
        self.__all_entities = [
            get_entity(entity_key) for entity_key in (all_entities or [default_entity])
        ]
        self.name = name
        self.__is_experimental = is_experimental or False

    def is_experimental(self) -> bool:
        return self.__is_experimental

    def get_all_entities(self) -> list[Entity]:
        return self.__all_entities

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, PluggableDataset) and self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)
