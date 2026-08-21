import time
from concurrent.futures import TimeoutError as FutureTimeoutError
from unittest import mock

from snuba.admin.clickhouse.clusters import (
    _cluster_targets,
    _ClusterState,
    _ClusterTarget,
    _get_cluster_state,
    _single_node_target_key,
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


def single_node_cluster(
    host: str,
    port: int,
    storage_sets: set[StorageSetKey],
    *,
    database: str = "default",
    secure: bool = False,
    ca_certs: str | None = None,
    verify: bool | None = None,
) -> mock.MagicMock:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.is_single_node.return_value = True
    cluster.get_host.return_value = host
    cluster.get_port.return_value = port
    cluster.get_database.return_value = database
    cluster.get_secure.return_value = secure
    cluster.get_ca_certs.return_value = ca_certs
    cluster.get_verify.return_value = verify
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
        _ClusterTarget(_single_node_target_key(first), "single node", first, {"events"}, True),
        _ClusterTarget(
            _single_node_target_key(second), "single node", second, {"transactions"}, True
        ),
    ]


@mock.patch("snuba.admin.clickhouse.clusters.CLUSTERS")
def test_cluster_targets_keeps_same_host_different_tls_settings_separate(
    clusters: mock.MagicMock,
) -> None:
    first = single_node_cluster(
        "clickhouse-a",
        9000,
        {StorageSetKey.EVENTS},
        secure=True,
        ca_certs="/etc/ssl/ca.pem",
        verify=True,
    )
    second = single_node_cluster(
        "clickhouse-a",
        9000,
        {StorageSetKey.TRANSACTIONS},
        secure=True,
        ca_certs="/etc/ssl/other-ca.pem",
        verify=False,
    )
    clusters.__iter__.return_value = iter([first, second])

    targets = _cluster_targets()

    assert _single_node_target_key(first) != _single_node_target_key(second)
    assert targets == [
        _ClusterTarget(_single_node_target_key(first), "single node", first, {"events"}, True),
        _ClusterTarget(
            _single_node_target_key(second), "single node", second, {"transactions"}, True
        ),
    ]


@mock.patch("snuba.admin.clickhouse.clusters.CLUSTERS")
def test_cluster_targets_merges_identical_single_node_connections(
    clusters: mock.MagicMock,
) -> None:
    # App credentials are unused by the admin RO path, so they must not split rows.
    first = single_node_cluster("clickhouse-a", 9000, {StorageSetKey.EVENTS})
    second = single_node_cluster("clickhouse-a", 9000, {StorageSetKey.TRANSACTIONS})
    clusters.__iter__.return_value = iter([first, second])

    targets = _cluster_targets()

    assert targets == [
        _ClusterTarget(
            _single_node_target_key(first),
            "single node",
            first,
            {"events", "transactions"},
            True,
        )
    ]


def _execute_by_sql(sql: str) -> mock.MagicMock:
    if "system.one" in sql:
        if "version()" in sql:
            return result([("24.8.14",), ("25.3.6",)])
        return result([])
    if "system.tables" in sql:
        return result([(["errors_local", "metrics_local", "shared"],)])
    raise AssertionError(f"unexpected sql: {sql}")


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_queries_versions_and_tables_across_all_replicas(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    query_node = ClickhouseNode("query", 8123)
    cluster.get_query_node.return_value = query_node
    target = _ClusterTarget("cluster_one", "cluster_one", cluster, {"events"}, False)
    connection = get_ro_cluster_node_connection.return_value
    connection.execute.side_effect = _execute_by_sql

    state = _get_cluster_state(target)

    assert state.versions == ["24.8.14", "25.3.6"]
    assert state.tables == ["errors_local", "metrics_local", "shared"]
    assert state.error is None
    assert get_ro_cluster_node_connection.call_count == 2
    get_ro_cluster_node_connection.assert_called_with(
        cluster,
        query_node,
        ClickhouseClientSettings.QUERY,
        known_nodes=[query_node],
    )
    sqls = [call.args[0] for call in connection.execute.call_args_list]
    version_sql = next(sql for sql in sqls if "version()" in sql)
    tables_sql = next(sql for sql in sqls if "system.tables" in sql)
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

    def execute(sql: str) -> mock.MagicMock:
        if "system.one" in sql:
            return result([])
        return result([([],)])

    get_ro_cluster_node_connection.return_value.execute.side_effect = execute

    _get_cluster_state(target)

    sqls = [
        call.args[0] for call in get_ro_cluster_node_connection.return_value.execute.call_args_list
    ]
    assert any("clusterAllReplicas('cluster\\'name', system.one)" in sql for sql in sqls)
    assert any("clusterAllReplicas('cluster\\'name', system.tables)" in sql for sql in sqls)


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_queries_single_node_locally(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.get_query_node.return_value = ClickhouseNode("single", 8123)
    target = _ClusterTarget("single:9000", "single", cluster, set(), True)

    def execute(sql: str) -> mock.MagicMock:
        if "system.one" in sql:
            return result([("25.3.6",)])
        return result([(["errors_local"],)])

    get_ro_cluster_node_connection.return_value.execute.side_effect = execute

    state = _get_cluster_state(target)

    assert state.versions == ["25.3.6"]
    assert state.tables == ["errors_local"]
    assert state.error is None
    sqls = [
        call.args[0] for call in get_ro_cluster_node_connection.return_value.execute.call_args_list
    ]
    version_sql = next(sql for sql in sqls if "system.one" in sql)
    tables_sql = next(sql for sql in sqls if "system.tables" in sql)
    assert "FROM system.one" in version_sql
    assert "FROM system.tables" in tables_sql
    assert "clusterAllReplicas" not in version_sql
    assert "clusterAllReplicas" not in tables_sql


@mock.patch("snuba.admin.clickhouse.clusters._get_cluster_state")
@mock.patch("snuba.admin.clickhouse.clusters.CLUSTER_QUERY_TIMEOUT", 10)
@mock.patch("snuba.admin.clickhouse.clusters.ThreadPoolExecutor")
@mock.patch("snuba.admin.clickhouse.clusters._cluster_targets")
def test_get_cluster_info_reports_timeout(
    cluster_targets: mock.MagicMock,
    executor: mock.MagicMock,
    get_cluster_state: mock.MagicMock,
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
    # Outer submit passes the shared deadline into each worker.
    args, _kwargs = executor.return_value.submit.call_args
    assert args[0] is get_cluster_state
    assert args[1] is target
    assert isinstance(args[2], float)


@mock.patch("snuba.admin.clickhouse.clusters._query_tables")
@mock.patch("snuba.admin.clickhouse.clusters._query_versions")
def test_get_cluster_state_honors_shared_deadline(
    query_versions: mock.MagicMock,
    query_tables: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    target = _ClusterTarget("cluster_one", "cluster_one", cluster, {"events"}, False)
    query_versions.side_effect = FutureTimeoutError
    query_tables.side_effect = FutureTimeoutError

    state = _get_cluster_state(target, deadline=time.monotonic() - 1)

    assert state == _ClusterState((), (), "Timed out after 30s")


@mock.patch("snuba.admin.clickhouse.clusters._query_tables")
@mock.patch("snuba.admin.clickhouse.clusters._query_versions")
def test_get_cluster_state_preserves_versions_when_tables_fail(
    query_versions: mock.MagicMock,
    query_tables: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    target = _ClusterTarget("cluster_one", "cluster_one", cluster, {"events"}, False)
    query_versions.return_value = ["25.3.6"]
    query_tables.side_effect = Exception("tables unavailable")

    state = _get_cluster_state(target)

    assert state == _ClusterState(["25.3.6"], (), "tables unavailable")


@mock.patch("snuba.admin.clickhouse.clusters._query_tables")
@mock.patch("snuba.admin.clickhouse.clusters._query_versions")
def test_get_cluster_state_preserves_tables_when_versions_fail(
    query_versions: mock.MagicMock,
    query_tables: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    target = _ClusterTarget("cluster_one", "cluster_one", cluster, {"events"}, False)
    query_versions.side_effect = Exception("versions unavailable")
    query_tables.return_value = ["errors_local"]

    state = _get_cluster_state(target)

    assert state == _ClusterState((), ["errors_local"], "versions unavailable")


@mock.patch("snuba.admin.clickhouse.clusters.ThreadPoolExecutor")
@mock.patch("snuba.admin.clickhouse.clusters._cluster_targets")
def test_get_cluster_info_returns_partial_state(
    cluster_targets: mock.MagicMock,
    executor: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    target = _ClusterTarget("cluster_one", "cluster_one", cluster, {"events"}, False)
    cluster_targets.return_value = [target]
    executor.return_value.submit.return_value.result.return_value = _ClusterState(
        ["25.3.6"], (), "tables unavailable"
    )

    info = get_cluster_info()[0]

    assert info == {
        "cluster_name": "cluster_one",
        "versions": ["25.3.6"],
        "storage_sets": ["events"],
        "tables": (),
        "error": "tables unavailable",
    }
