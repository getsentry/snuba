from __future__ import annotations

import sys
from typing import Any, Sequence

import yaml

from snuba.datasets.configuration.utils import serialize_columns
from snuba.datasets.storage import WritableStorage
from snuba.datasets.storages.factory import get_storage, initialize_storage_factory
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.processors.physical import ClickhouseQueryProcessor

initialize_storage_factory()


def convert_query_processors(
    qps: Sequence[ClickhouseQueryProcessor],
) -> list[dict[str, Any]]:
    res = []
    for qp in qps:
        processor_dict = {}
        processor_dict["processor"] = qp.config_key()
        if qp.init_kwargs:
            processor_dict["args"] = qp.init_kwargs
        res.append(processor_dict)

    return res


def convert_to_yaml(key: StorageKey, result_path):
    storage = get_storage(key)
    res = {
        "version": "v1",
        "kind": "writable_storage"
        if isinstance(storage, WritableStorage)
        else "readable_storage",
        "name": key.value,
        "storage": {"key": key.value, "set_key": storage.get_storage_set_key().value},
    }
    res["schema"] = {
        "columns": serialize_columns(storage.get_schema().get_columns().columns),
        "local_table_name": storage.get_schema().get_local_table_name(),
        "dist_table_name": storage.get_schema().get_table_name(),
        "partition_format": storage.get_schema().get_partition_format(),
    }

    res["query_processors"] = convert_query_processors(storage.get_query_processors())
    with open(result_path, "w") as f:
        yaml.dump(res, f, sort_keys=False)


if __name__ == "__main__":
    convert_to_yaml(StorageKey(sys.argv[1]), sys.argv[2])
