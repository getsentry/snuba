from __future__ import annotations

from typing import Sequence, TypedDict

from snuba import settings
from snuba.clusters.cluster import UndefinedClickhouseCluster
from snuba.clusters.storage_sets import DEV_STORAGE_SETS
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import STORAGES, get_storage

Node = TypedDict("Node", {"host": str, "port": int})

Storage = TypedDict(
    "Storage",
    {"storage_name": str, "local_table_name": str, "local_nodes": Sequence[Node]},
)


def _get_local_table_name(storage_key: StorageKey) -> str:
    schema = get_storage(storage_key).get_schema()
    assert isinstance(schema, TableSchema)
    return schema.get_table_name()


def _get_local_nodes(storage_key: StorageKey) -> Sequence[Node]:
    try:
        storage = get_storage(storage_key)
        return [
            {"host": node.host_name, "port": node.port}
            for node in storage.get_cluster().get_local_nodes()
        ]
    except (AssertionError, KeyError, UndefinedClickhouseCluster):
        # If cluster_name is not defined just return an empty list
        return []


def get_storage_info() -> Sequence[Storage]:
    return [
        {
            "storage_name": storage_key.value,
            "local_table_name": _get_local_table_name(storage_key),
            "local_nodes": _get_local_nodes(storage_key),
        }
        for storage_key in sorted(STORAGES, key=lambda storage_key: storage_key.value)
        if get_storage(storage_key).get_storage_set_key() not in DEV_STORAGE_SETS
        or settings.ENABLE_DEV_FEATURES
    ]
