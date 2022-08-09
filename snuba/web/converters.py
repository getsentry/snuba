from werkzeug.routing import BaseConverter

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey, EntityKeys
from snuba.datasets.entities.factory import ENTITY_NAME_LOOKUP, get_entity
from snuba.datasets.entity import Entity
from snuba.datasets.factory import get_dataset, get_dataset_name


class DatasetConverter(BaseConverter):
    def to_python(self, value: str) -> Dataset:
        return get_dataset(value)

    def to_url(self, value: Dataset) -> str:
        return get_dataset_name(value)


class EntityConverter(BaseConverter):
    def to_python(self, value: str) -> Entity:
        return get_entity(EntityKey(value))

    def to_url(self, value: Entity) -> str:
        return ENTITY_NAME_LOOKUP[value].value
