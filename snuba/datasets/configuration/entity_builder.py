from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.configuration.json_schema import V1_ENTITY_SCHEMA
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.entity import Entity
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage


def build_entity_from_config(file_path: str) -> Entity:
    config_data = load_configuration_data(file_path, {"entity": V1_ENTITY_SCHEMA})
    return PluggableEntity(
        query_processors=[],
        columns=[],
        readable_storage=get_storage(StorageKey(config_data["readable_storage"])),
        validators=[],
        translation_mappers=TranslationMappers(),
        writeable_storage=get_writable_storage(
            StorageKey(config_data["writable_storage"])
        )
        if config_data["writable_storage"]
        else None,
    )
