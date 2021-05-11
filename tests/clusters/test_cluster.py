import importlib
from unittest.mock import patch

import pytest

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters import cluster, storage_sets
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage

ENABLED_STORAGE_SETS = {
    "discover",
    "events",
    "events_ro",
    "metrics",
    "migrations",
    "querylog",
    "sessions",
}

ALL_STORAGE_SETS = {
    "outcomes",
    *ENABLED_STORAGE_SETS,
}

REDUCED_COPNFIG = [
    {
        "host": "host_1",
        "port": 9000,
        "user": "default",
        "password": "",
        "database": "default",
        "http_port": 8123,
        "storage_sets": ENABLED_STORAGE_SETS,
        "single_node": True,
    },
    {
        "host": "host_2",
        "port": 9000,
        "user": "default",
        "password": "",
        "database": "default",
        "http_port": 8123,
        "storage_sets": {"transactions"},
        "single_node": False,
        "cluster_name": "clickhouse_hosts",
        "distributed_cluster_name": "dist_hosts",
    },
]

FULL_CONFIG = [
    {
        "host": "host_1",
        "port": 9000,
        "user": "default",
        "password": "",
        "database": "default",
        "http_port": 8123,
        "storage_sets": ALL_STORAGE_SETS,
        "single_node": True,
    },
    {
        "host": "host_2",
        "port": 9000,
        "user": "default",
        "password": "",
        "database": "default",
        "http_port": 8123,
        "storage_sets": {"transactions"},
        "single_node": False,
        "cluster_name": "clickhouse_hosts",
        "distributed_cluster_name": "dist_hosts",
    },
]


def setup_function() -> None:
    storage_sets.DEV_STORAGE_SETS = frozenset(
        {
            StorageSetKey.OUTCOMES,  # Disabled and not registered
            StorageSetKey.QUERYLOG,  # Disabled still registered
        }
    )
    settings.CLUSTERS = REDUCED_COPNFIG
    importlib.reload(cluster)


def teardown_function() -> None:
    settings.CLUSTERS = FULL_CONFIG
    storage_sets.DEV_STORAGE_SETS = frozenset()

    importlib.reload(settings)
    importlib.reload(cluster)


def test_clusters() -> None:
    assert (
        get_storage(StorageKey("events")).get_cluster()
        == get_storage(StorageKey("errors")).get_cluster()
    )

    assert (
        get_storage(StorageKey("events")).get_cluster()
        != get_storage(StorageKey("transactions")).get_cluster()
    )


def test_disabled_cluster() -> None:
    with pytest.raises(AssertionError):
        cluster.get_cluster(StorageSetKey.OUTCOMES)

    settings.ENABLE_DEV_FEATURES = True
    settings.CLUSTERS = FULL_CONFIG
    importlib.reload(cluster)
    cluster.get_cluster(StorageSetKey.OUTCOMES)
    settings.ENABLE_DEV_FEATURES = False


def test_get_local_nodes() -> None:
    with patch.object(ClickhousePool, "execute") as execute:
        execute.return_value = [
            ("host_1", 9000, 1, 1),
            ("host_2", 9000, 2, 1),
        ]

        local_cluster = get_storage(StorageKey("events")).get_cluster()
        assert len(local_cluster.get_local_nodes()) == 1
        assert local_cluster.get_local_nodes()[0].host_name == "host_1"
        assert local_cluster.get_local_nodes()[0].port == 9000
        assert local_cluster.get_local_nodes()[0].shard is None
        assert local_cluster.get_local_nodes()[0].replica is None

        distributed_cluster = get_storage(StorageKey("transactions")).get_cluster()
        assert len(distributed_cluster.get_local_nodes()) == 2
        assert distributed_cluster.get_local_nodes()[0].host_name == "host_1"
        assert distributed_cluster.get_local_nodes()[1].host_name == "host_2"


def test_cache_connections() -> None:
    cluster_1 = cluster.ClickhouseCluster(
        "localhost", 8000, "default", "", "default", 8001, {"events"}, True
    )

    assert cluster_1.get_query_connection(
        cluster.ClickhouseClientSettings.QUERY
    ) == cluster_1.get_query_connection(cluster.ClickhouseClientSettings.QUERY)

    assert cluster_1.get_node_connection(
        cluster.ClickhouseClientSettings.OPTIMIZE,
        cluster.ClickhouseNode("localhost", 8002),
    ) == cluster_1.get_node_connection(
        cluster.ClickhouseClientSettings.OPTIMIZE,
        cluster.ClickhouseNode("localhost", 8002),
    )
