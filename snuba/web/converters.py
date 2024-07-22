from werkzeug.routing import BaseConverter

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity, get_entity_name
from snuba.datasets.entity import Entity
from snuba.datasets.factory import get_dataset, get_dataset_name
from snuba.datasets.storage import Storage, StorageKey
from snuba.datasets.storages.factory import get_storage


class DatasetConverter(BaseConverter):
    def to_python(self, value: str) -> Dataset:
        return get_dataset(value)

    def to_url(self, value: Dataset) -> str:
        return get_dataset_name(value)


class EntityConverter(BaseConverter):
    def to_python(self, value: str) -> Entity:
        return get_entity(EntityKey(value))

    def to_url(self, value: Entity) -> str:
        return get_entity_name(value).value


class StorageConverter(BaseConverter):
    def to_python(self, value: str) -> Storage:
        return get_storage(StorageKey(value))

    def to_url(self, value: Storage) -> str:
        return value.get_storage_key().value
