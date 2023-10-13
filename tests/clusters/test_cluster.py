import importlib
from typing import Generator
from unittest.mock import patch

import pytest

from snuba import settings
from snuba.clickhouse.native import ClickhousePool, ClickhouseResult
from snuba.clusters import cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey

ENABLED_STORAGE_SETS = {
    "cdc",
    "discover",
    "events",
    "events_ro",
    "metrics",
    "migrations",
    "querylog",
}

ALL_STORAGE_SETS = {
    "outcomes",
    *ENABLED_STORAGE_SETS,
}

REDUCED_CONFIG = [
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
        "cache_partition_id": "host_2_cache",
        "query_settings_prefix": "transactions",
    },
]

SLICED_CLUSTERS_CONFIG = [
    {
        "host": "host_slice",
        "port": 9000,
        "user": "default",
        "password": "",
        "database": "default",
        "http_port": 8123,
        "storage_set_slices": {("generic_metrics_distributions", 0)},
        "single_node": True,
    },
    {
        "host": "host_slice",
        "port": 9001,
        "user": "default",
        "password": "",
        "database": "slice_1_default",
        "http_port": 8124,
        "storage_set_slices": {("generic_metrics_distributions", 1)},
        "single_node": True,
    },
]


@pytest.fixture(autouse=True)
def setup_teardown(clickhouse_db: None) -> Generator[None, None, None]:
    yield
    importlib.reload(settings)
    importlib.reload(cluster)


@patch("snuba.settings.CLUSTERS", FULL_CONFIG)
@pytest.mark.clickhouse_db
def test_clusters() -> None:
    importlib.reload(cluster)
    assert (
        get_storage(StorageKey("errors")).get_cluster()
        == get_storage(StorageKey("errors_ro")).get_cluster()
    )

    assert (
        get_storage(StorageKey("errors")).get_cluster()
        != get_storage(StorageKey("transactions")).get_cluster()
    )


@patch("snuba.settings.CLUSTERS", FULL_CONFIG)
@pytest.mark.clickhouse_db
def test_cache_partition() -> None:
    get_storage(
        StorageKey("transactions")
    ).get_cluster().get_reader().cache_partition_id == "host_2_cache"

    get_storage(
        StorageKey("errors")
    ).get_cluster().get_reader().cache_partition_id is None


@patch("snuba.settings.CLUSTERS", FULL_CONFIG)
@pytest.mark.clickhouse_db
def test_query_settings_prefix() -> None:
    get_storage(
        StorageKey("transactions")
    ).get_cluster().get_reader().get_query_settings_prefix() == "transactions"

    get_storage(
        StorageKey("errors")
    ).get_cluster().get_reader().get_query_settings_prefix() is None


@patch("snuba.settings.CLUSTERS", FULL_CONFIG)
@patch(
    "snuba.clusters.storage_sets.DEV_STORAGE_SETS",
    frozenset(
        {
            StorageSetKey.OUTCOMES,  # Disabled and not registered
            StorageSetKey.QUERYLOG,  # Disabled still registered
        }
    ),
)
@pytest.mark.clickhouse_db
def test_disabled_cluster() -> None:
    importlib.reload(cluster)

    cluster.get_cluster(StorageSetKey.OUTCOMES)

    with patch("snuba.settings.ENABLE_DEV_FEATURES", False):
        with pytest.raises(AssertionError):
            cluster.get_cluster(StorageSetKey.OUTCOMES)


@patch("snuba.settings.CLUSTERS", FULL_CONFIG)
@pytest.mark.clickhouse_db
def test_get_local_nodes() -> None:
    importlib.reload(cluster)
    with patch.object(ClickhousePool, "execute") as execute:
        execute.return_value = ClickhouseResult(
            [("host_1", 9000, 1, 1), ("host_2", 9000, 2, 1)]
        )

        local_cluster = get_storage(StorageKey("errors")).get_cluster()
        assert len(local_cluster.get_local_nodes()) == 1
        assert local_cluster.get_local_nodes()[0].host_name == "host_1"
        assert local_cluster.get_local_nodes()[0].port == 9000
        assert local_cluster.get_local_nodes()[0].shard is None
        assert local_cluster.get_local_nodes()[0].replica is None

        distributed_cluster = get_storage(StorageKey("transactions")).get_cluster()
        assert len(distributed_cluster.get_local_nodes()) == 2
        assert distributed_cluster.get_local_nodes()[0].host_name == "host_1"
        assert distributed_cluster.get_local_nodes()[1].host_name == "host_2"


@pytest.mark.clickhouse_db
def test_cache_connections() -> None:
    cluster_1 = cluster.ClickhouseCluster(
        "127.0.0.1", 8000, "default", "", "default", 8001, {"events"}, True
    )

    cluster_2 = cluster.ClickhouseCluster(
        "127.0.0.1", 8000, "default", "", "default", 8001, {"transactions"}, True
    )

    # Same node but different user
    cluster_3 = cluster.ClickhouseCluster(
        "127.0.0.1", 8000, "readonly", "", "default", 8001, {"metrics"}, True
    )

    assert cluster_1.get_query_connection(
        cluster.ClickhouseClientSettings.QUERY
    ) == cluster_1.get_query_connection(cluster.ClickhouseClientSettings.QUERY)

    assert cluster_1.get_node_connection(
        cluster.ClickhouseClientSettings.OPTIMIZE,
        cluster.ClickhouseNode("127.0.0.1", 8002),
    ) == cluster_1.get_node_connection(
        cluster.ClickhouseClientSettings.OPTIMIZE,
        cluster.ClickhouseNode("127.0.0.1", 8002),
    )

    assert cluster_1.get_query_connection(
        cluster.ClickhouseClientSettings.QUERY
    ) == cluster_2.get_query_connection(cluster.ClickhouseClientSettings.QUERY)

    # Does not share a connection since user is different
    assert cluster_1.get_query_connection(
        cluster.ClickhouseClientSettings.QUERY
    ) != cluster_3.get_query_connection(cluster.ClickhouseClientSettings.QUERY)


@patch("snuba.settings.SLICED_CLUSTERS", SLICED_CLUSTERS_CONFIG)
@pytest.mark.clickhouse_db
def test_sliced_cluster() -> None:
    importlib.reload(cluster)

    res_cluster = cluster.get_cluster(StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS, 1)

    assert res_cluster.is_single_node() == True
    assert res_cluster.get_database() == "slice_1_default"
    assert res_cluster.get_host() == "host_slice"
    assert res_cluster.get_port() == 9001

    res_cluster_default = cluster.get_cluster(
        StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS, 0
    )

    assert res_cluster_default.is_single_node() == True
    assert res_cluster_default.get_database() == "default"
    assert res_cluster_default.get_host() == "host_slice"
    assert res_cluster_default.get_port() == 9000
