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

_FILE_MAP = {
    StorageKey.TRANSACTIONS: "snuba/datasets/configuration/transactions/storages/transactions.yaml",
    StorageKey.GENERIC_METRICS_DISTRIBUTIONS: "snuba/datasets/configuration/generic_metrics/storages/distributions.yaml",
}


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


def get_yaml_query_processors(filepath):
    with open(filepath) as f:
        l = yaml.safe_load(f.read())
        return l["query_processors"]


def check_equality(converted, file_qps):
    proc_difference = set([qp["processor"] for qp in converted]) - set(
        [qp["processor"] for qp in file_qps]
    )
    assert not proc_difference, proc_difference
    for i, proc in enumerate(file_qps):
        converted_qp = converted[i]
        file_qp = file_qps[i]
        if converted_qp["processor"] != file_qp["processor"]:
            raise Exception("bad ordering")
        if converted_qp.get("args", None) != file_qp.get("args", None):
            print("Arg mismatch")
            print(
                (
                    converted_qp["processor"],
                    converted_qp.get("args", None),
                    file_qp.get("args", None),
                )
            )


def check_against_real_storage(converted, original):
    # arrange original by name
    orig_by_name = {og_qp.config_key(): og_qp for og_qp in original}

    # create list of converted into actual processor
    converted_by_name = {
        qp["processor"]: ClickhouseQueryProcessor.get_from_name(
            qp["processor"]
        ).from_kwargs(**qp.get("args", {}))
        for qp in converted
    }

    assert set(orig_by_name.keys()) == set(converted_by_name.keys())

    for name in orig_by_name.keys():
        orig = orig_by_name[name]
        converted_thing = converted_by_name[name]
        assert orig.init_kwargs == converted_thing.init_kwargs


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
