from concurrent.futures import TimeoutError as FutureTimeoutError
from unittest import mock

from snuba.admin.clickhouse.clusters import (
    SINGLE_NODE_CLUSTER_NAME,
    _cluster_targets,
    _ClusterTarget,
    _get_cluster_state,
    get_cluster_info,
)
from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseCluster, ClickhouseNode
from snuba.clusters.storage_sets import StorageSetKey


def result(rows: list[tuple[object, ...]]) -> mock.MagicMock:
    value = mock.MagicMock()
    value.results = rows
    return value


def multi_node_cluster(
    query_name: str, storage_name: str, storage_sets: set[StorageSetKey]
) -> mock.MagicMock:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.is_single_node.return_value = False
    cluster.get_clickhouse_distributed_cluster_name.return_value = query_name
    cluster.get_clickhouse_cluster_name.return_value = storage_name
    cluster.get_storage_set_keys.return_value = storage_sets
    return cluster


@mock.patch("snuba.admin.clickhouse.clusters.CLUSTERS")
def test_cluster_targets_flattens_and_deduplicates_cluster_names(clusters: mock.MagicMock) -> None:
    first = multi_node_cluster("query_cluster", "shared_cluster", {StorageSetKey.EVENTS})
    second = multi_node_cluster("other_query", "shared_cluster", {StorageSetKey.TRANSACTIONS})
    clusters.__iter__.return_value = iter([first, second])

    targets = {target.name: target for target in _cluster_targets()}

    assert set(targets) == {"query_cluster", "other_query", "shared_cluster"}
    assert targets["shared_cluster"].storage_sets == {"events", "transactions"}


@mock.patch("snuba.admin.clickhouse.clusters.CLUSTERS")
def test_cluster_targets_names_single_node(clusters: mock.MagicMock) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.is_single_node.return_value = True
    cluster.get_storage_set_keys.return_value = {StorageSetKey.EVENTS}
    clusters.__iter__.return_value = iter([cluster])

    assert _cluster_targets() == [
        _ClusterTarget(SINGLE_NODE_CLUSTER_NAME, cluster, {"events"}, True)
    ]


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_queries_versions_and_tables_across_all_replicas(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    query_node = ClickhouseNode("query", 8123)
    cluster.get_query_node.return_value = query_node
    target = _ClusterTarget("cluster_one", cluster, {"events"}, False)
    connection = get_ro_cluster_node_connection.return_value
    connection.execute.return_value = result(
        [
            ("24.8.14", ["errors_local", "shared"]),
            ("25.3.6", ["metrics_local", "shared"]),
        ]
    )

    state = _get_cluster_state(target)

    assert state.versions == ["24.8.14", "25.3.6"]
    assert state.tables == ["errors_local", "metrics_local", "shared"]
    get_ro_cluster_node_connection.assert_called_once_with(
        cluster,
        query_node,
        ClickhouseClientSettings.QUERY,
        known_nodes=[query_node],
    )
    sql = connection.execute.call_args.args[0]
    assert "clusterAllReplicas('cluster_one', system.tables)" in sql
    assert "version() AS version" in sql
    assert "arraySort(groupUniqArray(name)) AS tables" in sql
    assert "WHERE database = 'default'" in sql
    assert "GROUP BY version" in sql


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_escapes_cluster_name(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.get_query_node.return_value = ClickhouseNode("query", 8123)
    target = _ClusterTarget("cluster'name", cluster, set(), False)
    get_ro_cluster_node_connection.return_value.execute.return_value = result([])

    _get_cluster_state(target)

    sql = get_ro_cluster_node_connection.return_value.execute.call_args.args[0]
    assert "clusterAllReplicas('cluster\\'name', system.tables)" in sql


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_queries_single_node_locally(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.get_query_node.return_value = ClickhouseNode("single", 8123)
    target = _ClusterTarget(SINGLE_NODE_CLUSTER_NAME, cluster, set(), True)
    get_ro_cluster_node_connection.return_value.execute.return_value = result(
        [("25.3.6", ["errors_local"])]
    )

    state = _get_cluster_state(target)

    assert state.versions == ["25.3.6"]
    assert state.tables == ["errors_local"]
    sql = get_ro_cluster_node_connection.return_value.execute.call_args.args[0]
    assert "FROM system.tables" in sql
    assert "clusterAllReplicas" not in sql


@mock.patch("snuba.admin.clickhouse.clusters.CLUSTER_QUERY_TIMEOUT", 10)
@mock.patch("snuba.admin.clickhouse.clusters.ThreadPoolExecutor")
@mock.patch("snuba.admin.clickhouse.clusters._cluster_targets")
def test_get_cluster_info_reports_timeout(
    cluster_targets: mock.MagicMock,
    executor: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    target = _ClusterTarget("cluster_one", cluster, {"events"}, False)
    cluster_targets.return_value = [target]
    executor.return_value.submit.return_value.result.side_effect = FutureTimeoutError

    info = get_cluster_info()[0]

    assert info["cluster_name"] == "cluster_one"
    assert info["versions"] == ()
    assert info["tables"] == ()
    assert info["error"] == "Timed out after 10s"
