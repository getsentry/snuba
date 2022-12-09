from __future__ import annotations

import sys
from dataclasses import fields
from typing import Any, Sequence

import yaml

from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.configuration.utils import serialize_columns
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity, reset_entity_factory
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder

reset_entity_factory()


def _convert_registered_class(cls: Any, name: str) -> dict[str, Any]:
    res = {}
    res[name] = cls.config_key()
    if cls.init_kwargs:
        res["args"] = cls.init_kwargs
    return res


def _convert_registered_classes(
    cls_list: Sequence[Any], name: str
) -> list[dict[str, Any]]:
    res = []
    for cls in cls_list:
        res.append(_convert_registered_class(cls, name))

    return res


def _convert_data_model(column_set: EntityColumnSet) -> list:
    return serialize_columns(column_set.columns)
    # return str(serialize_columns(column_set.columns)).replace("'", "")


def _convert_mappers(mappers: TranslationMappers):
    res = {}
    for field in fields(mappers):
        fname = field.name
        field_mappers = getattr(mappers, fname)
        res[fname] = _convert_registered_classes(field_mappers, "mapper")
    return res


def convert_to_yaml(key: EntityKey, result_path: str) -> None:
    entity = get_entity(key)
    res: dict[str, Any] = {
        "version": "v1",
        "kind": "entity",
        "name": key.value,
    }
    res["schema"] = _convert_data_model(entity.get_data_model())
    assert (
        len(entity.get_all_storages()) == 1
    ), "why are there multiple storages on this entity? deal with it"
    storage = entity.get_writable_storage()
    if not storage:
        storage = entity.get_all_storages()[0]
        res["readable_storage"] = storage.get_storage_key().value
    else:
        res["readable_storage"] = storage.get_storage_key().value
        res["writable_storage"] = storage.get_storage_key().value
    if processors := _convert_registered_classes(
        entity.get_query_processors(), "processor"
    ):
        res["query_processors"] = processors

    if processors := _convert_registered_classes(
        entity.get_query_processors(), "processor"
    ):
        res["query_processors"] = processors

    pipeline_builder = entity.get_query_pipeline_builder()
    assert isinstance(pipeline_builder, SimplePipelineBuilder)
    if mappers := pipeline_builder.query_plan_builder.mappers:
        res["translation_mappers"] = _convert_mappers(mappers)

    with open(result_path, "w") as f:
        yaml.dump(res, f, sort_keys=False)


if __name__ == "__main__":
    convert_to_yaml(EntityKey(sys.argv[1]), sys.argv[2])
