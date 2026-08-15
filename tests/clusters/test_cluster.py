import importlib
from collections.abc import Generator
from unittest.mock import patch

import pytest

from snuba import settings
from snuba.clickhouse.connect import ClickhouseConnectPool
from snuba.clickhouse.pool import ClickhouseResult
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
        "storage_sets": ENABLED_STORAGE_SETS,
        "single_node": True,
    },
    {
        "host": "host_2",
        "port": 9000,
        "user": "default",
        "password": "",
        "database": "default",
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
        "secure": False,
        "ca_certs": None,
        "verify": False,
        "storage_sets": ALL_STORAGE_SETS,
        "single_node": True,
    },
    {
        "host": "host_2",
        "port": 9000,
        "user": "default",
        "password": "",
        "database": "default",
        "secure": False,
        "ca_certs": None,
        "verify": False,
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
        "secure": False,
        "storage_set_slices": {("generic_metrics_counters", 0)},
        "single_node": True,
    },
    {
        "host": "host_slice",
        "port": 9001,
        "user": "default",
        "password": "",
        "database": "slice_1_default",
        "secure": False,
        "storage_set_slices": {("generic_metrics_counters", 1)},
        "single_node": True,
    },
]


@pytest.fixture(autouse=True)
def setup_teardown(clickhouse_db: None) -> Generator[None]:
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
    importlib.reload(cluster)
    assert (
        get_storage(StorageKey("transactions")).get_cluster().get_reader().cache_partition_id
        == "host_2_cache"
    )

    assert get_storage(StorageKey("errors")).get_cluster().get_reader().cache_partition_id is None


@patch("snuba.settings.CLUSTERS", FULL_CONFIG)
@pytest.mark.clickhouse_db
def test_query_settings_prefix() -> None:
    importlib.reload(cluster)
    assert (
        get_storage(StorageKey("transactions"))
        .get_cluster()
        .get_reader()
        .get_query_settings_prefix()
        == "transactions"
    )

    assert (
        get_storage(StorageKey("errors")).get_cluster().get_reader().get_query_settings_prefix()
        is None
    )


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

    with patch("snuba.settings.ENABLE_DEV_FEATURES", False), pytest.raises(AssertionError):
        cluster.get_cluster(StorageSetKey.OUTCOMES)


@patch("snuba.settings.CLUSTERS", FULL_CONFIG)
@pytest.mark.clickhouse_db
def test_get_local_nodes() -> None:
    importlib.reload(cluster)
    with patch.object(ClickhouseConnectPool, "execute") as execute:
        execute.return_value = ClickhouseResult([("host_1", 9000, 1, 1), ("host_2", 9000, 2, 1)])

        local_cluster = get_storage(StorageKey("errors")).get_cluster()
        assert len(local_cluster.get_local_nodes()) == 1
        assert local_cluster.get_local_nodes()[0].host_name == "host_1"
        # Single-node clusters return the configured query-node port (FULL_CONFIG
        # still uses the legacy 9000 fixture value). Discovered multi-node replicas
        # rewrite to DEFAULT_CLICKHOUSE_HTTP_PORT (8123) instead.
        assert local_cluster.get_local_nodes()[0].port == 9000
        assert local_cluster.get_local_nodes()[0].shard is None
        assert local_cluster.get_local_nodes()[0].replica is None

        distributed_cluster = get_storage(StorageKey("transactions")).get_cluster()
        assert len(distributed_cluster.get_local_nodes()) == 2
        assert distributed_cluster.get_local_nodes()[0].host_name == "host_1"
        assert distributed_cluster.get_local_nodes()[1].host_name == "host_2"


@pytest.mark.clickhouse_db
def test_discovered_nodes_use_default_http_port() -> None:
    # The cluster's configured port is an Envoy intercept port that only
    # fronts the cluster endpoint (query node). Nodes discovered via
    # system.clusters are addressed directly (bypassing Envoy) and must carry
    # the well-known default HTTP port instead.
    envoy_port = 8158
    distributed_cluster = cluster.ClickhouseCluster(
        "host_query",
        envoy_port,
        "default",
        "",
        "default",
        False,
        None,
        False,
        {"events"},
        False,
        cluster_name="clickhouse_hosts",
        distributed_cluster_name="dist_hosts",
    )

    with patch.object(ClickhouseConnectPool, "execute") as execute:
        execute.return_value = ClickhouseResult([("host_1", 9000, 1, 1), ("host_2", 9000, 2, 1)])
        local_nodes = distributed_cluster.get_local_nodes()

    # The cluster endpoint keeps the Envoy intercept port ...
    assert distributed_cluster.get_port() == envoy_port
    # ... but directly-addressed nodes use the default HTTP port.
    assert len(local_nodes) == 2
    assert all(node.port == cluster.DEFAULT_CLICKHOUSE_HTTP_PORT for node in local_nodes)


@pytest.mark.clickhouse_db
def test_build_pool_uses_cluster_credentials() -> None:
    cluster_1 = cluster.ClickhouseCluster(
        "127.0.0.1",
        8001,
        "default",
        "",
        "default",
        False,
        None,
        False,
        {"events"},
        True,
    )
    cluster_ro = cluster.ClickhouseCluster(
        "127.0.0.1",
        8001,
        "readonly",
        "",
        "default",
        False,
        None,
        False,
        {"metrics"},
        True,
    )

    pool = cluster_1.get_query_connection(cluster.ClickhouseClientSettings.QUERY)
    assert pool.host == "127.0.0.1"
    assert pool.port == 8001
    assert pool.user == "default"
    assert (
        cluster_ro.get_query_connection(cluster.ClickhouseClientSettings.QUERY).user == "readonly"
    )


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_node_connection_uses_connect_pool() -> None:
    from snuba.clickhouse.connect import ClickhouseConnectPool
    from snuba.clickhouse.reader import ClickhouseReader

    test_cluster = cluster.ClickhouseCluster(
        "127.0.0.1",
        8001,
        "default",
        "",
        "default",
        False,
        None,
        False,
        {"events"},
        True,
    )

    pool = test_cluster.get_query_connection(cluster.ClickhouseClientSettings.QUERY)
    assert isinstance(pool, ClickhouseConnectPool)
    assert isinstance(test_cluster.get_reader(), ClickhouseReader)


@patch("snuba.settings.SLICED_CLUSTERS", SLICED_CLUSTERS_CONFIG)
@pytest.mark.clickhouse_db
def test_sliced_cluster() -> None:
    importlib.reload(cluster)

    res_cluster = cluster.get_cluster(StorageSetKey.GENERIC_METRICS_COUNTERS, 1)

    assert res_cluster.is_single_node()
    assert res_cluster.get_database() == "slice_1_default"
    assert res_cluster.get_host() == "host_slice"
    assert res_cluster.get_port() == 9001

    res_cluster_default = cluster.get_cluster(StorageSetKey.GENERIC_METRICS_COUNTERS, 0)

    assert res_cluster_default.is_single_node()
    assert res_cluster_default.get_database() == "default"
    assert res_cluster_default.get_host() == "host_slice"
    assert res_cluster_default.get_port() == 9000
