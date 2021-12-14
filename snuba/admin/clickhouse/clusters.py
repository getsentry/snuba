from __future__ import annotations

from typing import Sequence, TypedDict

from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import STORAGES, get_storage

Storage = TypedDict(
    "Storage",
    {"storage_name": str, "local_table_name": str, "local_nodes": Sequence[str]},
)


def _get_local_table_name(storage_key: StorageKey) -> str:
    schema = get_storage(storage_key).get_schema()
    assert isinstance(schema, TableSchema)
    return schema.get_table_name()


def _get_local_nodes(storage_key: StorageKey) -> Sequence[str]:
    storage = get_storage(storage_key)
    return [
        f"{node.host_name}:{node.port}"
        for node in storage.get_cluster().get_local_nodes()
    ]


def get_storage_info() -> Sequence[Storage]:
    return [
        {
            "storage_name": storage_key.value,
            "local_table_name": _get_local_table_name(storage_key),
            "local_nodes": _get_local_nodes(storage_key),
        }
        for storage_key in STORAGES
    ]
