from __future__ import annotations

from typing import Sequence, TypedDict

from snuba import settings
from snuba.clusters.cluster import UndefinedClickhouseCluster
from snuba.clusters.storage_sets import DEV_STORAGE_SETS
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage
from snuba.datasets.storages.storage_key import StorageKey

Node = TypedDict("Node", {"host": str, "port": int})

Storage = TypedDict(
    "Storage",
    {
        "storage_name": str,
        "local_table_name": str,
        "local_nodes": Sequence[Node],
        "dist_nodes": Sequence[Node],
    },
)


def _get_local_table_name(storage_key: StorageKey) -> str:
    try:
        schema = get_storage(storage_key).get_schema()
        assert isinstance(schema, TableSchema)
        return schema.get_table_name()
    except UndefinedClickhouseCluster:
        return "badcluster"


def _get_nodes(storage_key: StorageKey, local: bool = True) -> Sequence[Node]:
    try:
        storage = get_storage(storage_key)
        cluster = storage.get_cluster()
        return [
            {"host": node.host_name, "port": node.port}
            for node in (
                cluster.get_local_nodes() if local else cluster.get_distributed_nodes()
            )
        ]
    except (AssertionError, KeyError, UndefinedClickhouseCluster):
        # If cluster_name is not defined just return an empty list
        return []


def _get_local_nodes(storage_key: StorageKey) -> Sequence[Node]:
    return _get_nodes(storage_key, local=True)


def _get_dist_nodes(storage_key: StorageKey) -> Sequence[Node]:
    return _get_nodes(storage_key, local=False)


def get_storage_info() -> Sequence[Storage]:
    return [
        {
            "storage_name": storage_key.value,
            "local_table_name": _get_local_table_name(storage_key),
            "local_nodes": _get_local_nodes(storage_key),
            "dist_nodes": _get_dist_nodes(storage_key),
        }
        for storage_key in sorted(
            get_all_storage_keys(), key=lambda storage_key: storage_key.value
        )
        if get_storage(storage_key).get_storage_set_key() not in DEV_STORAGE_SETS
        or settings.ENABLE_DEV_FEATURES
    ]
