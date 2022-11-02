from __future__ import annotations

import sys
from typing import Any, Sequence

import yaml

from snuba.datasets.configuration.storage_builder import build_storage_from_config
from snuba.datasets.configuration.utils import serialize_columns
from snuba.datasets.storage import ReadableStorage
from snuba.datasets.storages.factory import get_storage, initialize_storage_factory
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.processors.physical import ClickhouseQueryProcessor


def _check_query_processors(converted, original) -> None:
    orig_by_name = {og_qp.config_key(): og_qp for og_qp in original}
    converted_by_name = {og_qp.config_key(): og_qp for og_qp in converted}

    assert set(orig_by_name.keys()) == set(converted_by_name.keys())

    for name in orig_by_name.keys():
        orig = orig_by_name[name]
        converted_thing = converted_by_name[name]
        assert orig.init_kwargs == converted_thing.init_kwargs


def _check_columns(converted, original):
    assert len(converted) == len(original)
    for i, orig_col in enumerate(original):
        converted_col = converted[i]
        assert orig_col == converted_col


def check_against_real_storage(
    converted_storage: ReadableStorage, original_storage: ReadableStorage
):
    _check_columns(
        converted_storage.get_schema().get_columns().columns,
        original_storage.get_schema().get_columns().columns,
    )
    _check_query_processors(
        converted_storage.get_query_processors(),
        original_storage.get_query_processors(),
    )


def check_yaml_against_code(storage_key: str, yaml_file_path: str):
    original_storage = get_storage(StorageKey(storage_key))
    converted_storage = build_storage_from_config(yaml_file_path)
    check_against_real_storage(converted_storage, original_storage)


if __name__ == "__main__":
    check_yaml_against_code(sys.argv[1], sys.argv[2])
