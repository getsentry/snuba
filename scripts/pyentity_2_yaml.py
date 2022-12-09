from __future__ import annotations

import sys
from typing import Any, Sequence

import yaml

from snuba.datasets.configuration.utils import serialize_columns
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity, reset_entity_factory

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


def _convert_data_model(column_set: EntityColumnSet) -> str:
    return str(serialize_columns(column_set.columns)).replace("'", "")


def convert_to_yaml(key: EntityKey, result_path: str) -> None:
    entity = get_entity(key)
    res: dict[str, Any] = {
        "version": "v1",
        "kind": "entity",
        "name": key.value,
    }
    res["schema"] = _convert_data_model(entity.get_data_model())

    with open(result_path, "w") as f:
        yaml.dump(res, f, sort_keys=False)


if __name__ == "__main__":
    convert_to_yaml(EntityKey(sys.argv[1]), sys.argv[2])
