import importlib
from unittest.mock import patch

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters import cluster
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage


def setup_function() -> None:
    settings.CLUSTERS = [
        {
            "host": "host_1",
            "port": 9000,
            "user": "default",
            "password": "",
            "database": "default",
            "http_port": 8123,
            "storage_sets": {
                "events",
                "events_ro",
                "migrations",
                "outcomes",
                "querylog",
                "sessions",
            },
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
    importlib.reload(cluster)


def teardown_function() -> None:
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
