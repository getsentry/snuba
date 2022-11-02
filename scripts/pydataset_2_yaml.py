from __future__ import annotations

import sys
from typing import Any, Sequence

import yaml

from snuba.datasets.configuration.utils import serialize_columns
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


def convert_to_yaml(key: StorageKey):
    storage = get_storage(key)
    converted = convert_query_processors(storage.get_query_processors())
    check_against_real_storage(converted, storage.get_query_processors())


if __name__ == "__main__":
    convert_to_yaml(StorageKey(sys.argv[1]))
