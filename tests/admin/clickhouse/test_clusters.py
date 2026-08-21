from concurrent.futures import TimeoutError as FutureTimeoutError
from unittest import mock

from snuba.admin.clickhouse.clusters import (
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


def single_node_cluster(host: str, port: int, storage_sets: set[StorageSetKey]) -> mock.MagicMock:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.is_single_node.return_value = True
    cluster.get_host.return_value = host
    cluster.get_port.return_value = port
    cluster.get_storage_set_keys.return_value = storage_sets
    return cluster


@mock.patch("snuba.admin.clickhouse.clusters.CLUSTERS")
def test_cluster_targets_flattens_and_deduplicates_cluster_names(clusters: mock.MagicMock) -> None:
    first = multi_node_cluster("query_cluster", "shared_cluster", {StorageSetKey.EVENTS})
    second = multi_node_cluster("other_query", "shared_cluster", {StorageSetKey.TRANSACTIONS})
    clusters.__iter__.return_value = iter([first, second])

    targets = {target.key: target for target in _cluster_targets()}

    assert set(targets) == {"query_cluster", "other_query", "shared_cluster"}
    assert targets["query_cluster"].storage_sets == {"events"}
    assert targets["other_query"].storage_sets == {"transactions"}
    assert targets["shared_cluster"].storage_sets == {"events", "transactions"}
    # Sibling query/storage targets from one Snuba cluster must not share a set.
    assert targets["query_cluster"].storage_sets is not targets["shared_cluster"].storage_sets


@mock.patch("snuba.admin.clickhouse.clusters.CLUSTERS")
def test_cluster_targets_keeps_single_node_hosts_separate(clusters: mock.MagicMock) -> None:
    first = single_node_cluster("clickhouse-a", 9000, {StorageSetKey.EVENTS})
    second = single_node_cluster("clickhouse-b", 9000, {StorageSetKey.TRANSACTIONS})
    clusters.__iter__.return_value = iter([first, second])

    targets = _cluster_targets()

    assert targets == [
        _ClusterTarget("clickhouse-a:9000", "single node", first, {"events"}, True),
        _ClusterTarget("clickhouse-b:9000", "single node", second, {"transactions"}, True),
    ]


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_queries_versions_and_tables_across_all_replicas(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    query_node = ClickhouseNode("query", 8123)
    cluster.get_query_node.return_value = query_node
    target = _ClusterTarget("cluster_one", "cluster_one", cluster, {"events"}, False)
    connection = get_ro_cluster_node_connection.return_value
    connection.execute.side_effect = [
        # Versions come from system.one so empty default DBs still report.
        result([("24.8.14",), ("25.3.6",)]),
        result([(["errors_local", "metrics_local", "shared"],)]),
    ]

    state = _get_cluster_state(target)

    assert state.versions == ["24.8.14", "25.3.6"]
    assert state.tables == ["errors_local", "metrics_local", "shared"]
    get_ro_cluster_node_connection.assert_called_once_with(
        cluster,
        query_node,
        ClickhouseClientSettings.QUERY,
        known_nodes=[query_node],
    )
    version_sql, tables_sql = [call.args[0] for call in connection.execute.call_args_list]
    assert "clusterAllReplicas('cluster_one', system.one)" in version_sql
    assert "SELECT DISTINCT version() AS version" in version_sql
    assert "clusterAllReplicas('cluster_one', system.tables)" in tables_sql
    assert "arraySort(groupUniqArray(name)) AS tables" in tables_sql
    assert "WHERE database = 'default'" in tables_sql


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_escapes_cluster_name(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.get_query_node.return_value = ClickhouseNode("query", 8123)
    target = _ClusterTarget("cluster'name", "cluster'name", cluster, set(), False)
    get_ro_cluster_node_connection.return_value.execute.side_effect = [
        result([]),
        result([([],)]),
    ]

    _get_cluster_state(target)

    version_sql, tables_sql = [
        call.args[0] for call in get_ro_cluster_node_connection.return_value.execute.call_args_list
    ]
    assert "clusterAllReplicas('cluster\\'name', system.one)" in version_sql
    assert "clusterAllReplicas('cluster\\'name', system.tables)" in tables_sql


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_queries_single_node_locally(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.get_query_node.return_value = ClickhouseNode("single", 8123)
    target = _ClusterTarget("single:9000", "single", cluster, set(), True)
    get_ro_cluster_node_connection.return_value.execute.side_effect = [
        result([("25.3.6",)]),
        result([(["errors_local"],)]),
    ]

    state = _get_cluster_state(target)

    assert state.versions == ["25.3.6"]
    assert state.tables == ["errors_local"]
    version_sql, tables_sql = [
        call.args[0] for call in get_ro_cluster_node_connection.return_value.execute.call_args_list
    ]
    assert "FROM system.one" in version_sql
    assert "FROM system.tables" in tables_sql
    assert "clusterAllReplicas" not in version_sql
    assert "clusterAllReplicas" not in tables_sql


@mock.patch("snuba.admin.clickhouse.clusters.CLUSTER_QUERY_TIMEOUT", 10)
@mock.patch("snuba.admin.clickhouse.clusters.ThreadPoolExecutor")
@mock.patch("snuba.admin.clickhouse.clusters._cluster_targets")
def test_get_cluster_info_reports_timeout(
    cluster_targets: mock.MagicMock,
    executor: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    target = _ClusterTarget("cluster_one", "cluster_one", cluster, {"events"}, False)
    cluster_targets.return_value = [target]
    executor.return_value.submit.return_value.result.side_effect = FutureTimeoutError

    info = get_cluster_info()[0]

    assert set(info) == {"cluster_name", "versions", "storage_sets", "tables", "error"}
    assert info["cluster_name"] == "cluster_one"
    assert info["versions"] == ()
    assert info["tables"] == ()
    assert info["error"] == "Timed out after 10s"
