from concurrent.futures import TimeoutError as FutureTimeoutError
from unittest import mock

from snuba.admin.clickhouse.clusters import _get_cluster_state, _get_versions, get_cluster_info
from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseCluster, ClickhouseNode


def result(rows: list[tuple[object, ...]]) -> mock.MagicMock:
    value = mock.MagicMock()
    value.results = rows
    return value


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_versions_queries_distinct_versions_on_all_cluster_replicas(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    query_node = ClickhouseNode("query", 8123)
    cluster.get_query_node.return_value = query_node
    connection = get_ro_cluster_node_connection.return_value
    connection.execute.return_value = result([("24.8.14",), ("25.3.6",)])

    versions = _get_versions(cluster, "query_cluster")

    assert versions == ["24.8.14", "25.3.6"]
    get_ro_cluster_node_connection.assert_called_once_with(
        cluster,
        query_node,
        ClickhouseClientSettings.QUERY,
        known_nodes=[query_node],
    )
    connection.execute.assert_called_once_with(
        "SELECT DISTINCT version() AS version FROM "
        "clusterAllReplicas('query_cluster', system.one) ORDER BY version"
    )


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_versions_escapes_cluster_name(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.get_query_node.return_value = ClickhouseNode("query", 8123)
    connection = get_ro_cluster_node_connection.return_value
    connection.execute.return_value = result([])

    _get_versions(cluster, "cluster'name")

    connection.execute.assert_called_once_with(
        "SELECT DISTINCT version() AS version FROM "
        "clusterAllReplicas('cluster\\'name', system.one) ORDER BY version"
    )


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_queries_query_and_storage_clusters(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.is_single_node.return_value = False
    cluster.get_query_node.return_value = ClickhouseNode("query", 8123)
    cluster.get_clickhouse_distributed_cluster_name.return_value = "query_cluster"
    cluster.get_clickhouse_cluster_name.return_value = "storage_cluster"

    def execute(sql: str, *args: object, **kwargs: object) -> mock.MagicMock:
        if "query_cluster" in sql:
            return result([("25.3.6",)])
        if "storage_cluster" in sql:
            return result([("24.8.14",), ("25.3.6",)])
        return result([(["migrations_local"],)])

    get_ro_cluster_node_connection.return_value.execute.side_effect = execute

    state = _get_cluster_state(cluster)

    assert state.query_cluster_versions == ["25.3.6"]
    assert state.storage_cluster_versions == ["24.8.14", "25.3.6"]
    assert state.query_node_error is None
    assert state.storage_node_error is None
    assert state.tables == ["migrations_local"]


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_queries_single_node_once(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.is_single_node.return_value = True
    cluster.get_query_node.return_value = ClickhouseNode("single", 8123)

    def execute(sql: str, *args: object, **kwargs: object) -> mock.MagicMock:
        if "version()" in sql:
            return result([("25.3.6",)])
        return result([(["migrations_local"],)])

    get_ro_cluster_node_connection.return_value.execute.side_effect = execute

    state = _get_cluster_state(cluster)

    assert state.query_cluster_versions == ["25.3.6"]
    assert state.storage_cluster_versions == ["25.3.6"]
    version_queries = [
        call.args[0]
        for call in get_ro_cluster_node_connection.return_value.execute.call_args_list
        if "version()" in call.args[0]
    ]
    assert version_queries == [
        "SELECT DISTINCT version() AS version FROM system.one ORDER BY version"
    ]


@mock.patch("snuba.admin.clickhouse.clusters.CLUSTER_QUERY_TIMEOUT", 10)
@mock.patch("snuba.admin.clickhouse.clusters.ThreadPoolExecutor")
def test_get_cluster_info_propagates_timeout_to_version_errors(
    executor: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.get_storage_set_keys.return_value = set()
    future = executor.return_value.submit.return_value
    future.result.side_effect = FutureTimeoutError

    with mock.patch("snuba.admin.clickhouse.clusters.CLUSTERS", [cluster]):
        info = get_cluster_info()[0]

    assert info["query_node_error"] == "Timed out after 10s"
    assert info["storage_node_error"] == "Timed out after 10s"
    assert info["error"] == "Timed out after 10s"


@mock.patch("snuba.admin.clickhouse.clusters._get_versions")
@mock.patch("snuba.admin.clickhouse.clusters._get_tables")
def test_get_cluster_state_isolates_cluster_version_errors(
    get_tables: mock.MagicMock,
    get_versions: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.is_single_node.return_value = False
    cluster.get_clickhouse_distributed_cluster_name.return_value = "query_cluster"
    cluster.get_clickhouse_cluster_name.return_value = "storage_cluster"

    def versions_for_cluster(cluster_arg: ClickhouseCluster, cluster_name: str | None) -> list[str]:
        if cluster_name == "query_cluster":
            raise RuntimeError("query unavailable")
        return ["24.8.14"]

    get_versions.side_effect = versions_for_cluster
    get_tables.return_value = ["errors_local"]

    state = _get_cluster_state(cluster)

    assert state.query_cluster_versions == []
    assert state.query_node_error == "query unavailable"
    assert state.storage_cluster_versions == ["24.8.14"]
    assert state.storage_node_error is None
    assert state.tables == ["errors_local"]
