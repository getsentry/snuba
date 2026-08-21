from concurrent.futures import TimeoutError as FutureTimeoutError
from unittest import mock

from snuba.admin.clickhouse.clusters import _get_cluster_state, get_cluster_info
from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseCluster, ClickhouseNode


def result(rows: list[tuple[object, ...]]) -> mock.MagicMock:
    value = mock.MagicMock()
    value.results = rows
    return value


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_queries_single_node_through_validated_helper(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    node = ClickhouseNode("single", 8123)
    cluster.is_single_node.return_value = True
    cluster.get_query_node.return_value = node

    def execute(sql: str, *args: object, **kwargs: object) -> mock.MagicMock:
        if "version()" in sql:
            return result([("25.3.6",)])
        return result([(["migrations_local"],)])

    connection = mock.MagicMock()
    connection.execute.side_effect = execute
    get_ro_cluster_node_connection.return_value = connection

    state = _get_cluster_state(cluster)

    assert get_ro_cluster_node_connection.call_count == 2
    assert all(
        call == mock.call(cluster, node, ClickhouseClientSettings.QUERY)
        for call in get_ro_cluster_node_connection.call_args_list
    )
    cluster.get_node_connection.assert_not_called()
    cluster.get_query_connection.assert_not_called()
    assert state.query_node_versions == [
        {"host": "single", "port": 8123, "version": "25.3.6", "error": None}
    ]
    assert state.storage_node_versions == state.query_node_versions
    assert state.tables == ["migrations_local"]


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_separates_query_and_storage_node_versions(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    query_node = ClickhouseNode("query", 8123)
    storage_node = ClickhouseNode("storage", 8123)
    cluster.is_single_node.return_value = False
    cluster.get_distributed_nodes.return_value = [query_node]
    cluster.get_local_nodes.return_value = [storage_node]
    cluster.get_query_node.return_value = query_node

    def connection_for_node(
        cluster_arg: ClickhouseCluster, node: ClickhouseNode, client_settings: object
    ) -> mock.MagicMock:
        connection = mock.MagicMock()

        def execute(sql: str, *args: object, **kwargs: object) -> mock.MagicMock:
            if "version()" in sql:
                if node.host_name == "query":
                    return result([("25.3.6",)])
                return result([("24.8.14",)])
            return result([([],)])

        connection.execute.side_effect = execute
        return connection

    get_ro_cluster_node_connection.side_effect = connection_for_node

    state = _get_cluster_state(cluster)

    assert state.query_node_versions[0]["version"] == "25.3.6"
    assert state.query_node_versions[0]["host"] == "query"
    assert state.storage_node_versions[0]["version"] == "24.8.14"
    assert state.storage_node_versions[0]["host"] == "storage"
    cluster.get_node_connection.assert_not_called()
    cluster.get_query_connection.assert_not_called()


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


@mock.patch("snuba.admin.clickhouse.clusters.ThreadPoolExecutor")
def test_get_cluster_info_propagates_unexpected_errors_to_version_fields(
    executor: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    cluster.get_storage_set_keys.return_value = set()
    future = executor.return_value.submit.return_value
    future.result.side_effect = RuntimeError("boom")

    with mock.patch("snuba.admin.clickhouse.clusters.CLUSTERS", [cluster]):
        info = get_cluster_info()[0]

    assert info["query_node_error"] == "boom"
    assert info["storage_node_error"] == "boom"
    assert info["error"] == "boom"


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_isolates_node_version_errors(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    query_node = ClickhouseNode("query", 8123)
    storage_node = ClickhouseNode("storage", 8123)
    cluster.is_single_node.return_value = False
    cluster.get_distributed_nodes.return_value = [query_node]
    cluster.get_local_nodes.return_value = [storage_node]
    cluster.get_query_node.return_value = query_node

    def connection_for_node(
        cluster_arg: ClickhouseCluster, node: ClickhouseNode, client_settings: object
    ) -> mock.MagicMock:
        connection = mock.MagicMock()

        def execute(sql: str, *args: object, **kwargs: object) -> mock.MagicMock:
            if "version()" in sql:
                if node.host_name == "query":
                    raise Exception("query unavailable")
                return result([("24.8.14",)])
            return result([([],)])

        connection.execute.side_effect = execute
        return connection

    get_ro_cluster_node_connection.side_effect = connection_for_node

    state = _get_cluster_state(cluster)

    assert state.query_node_versions[0]["version"] is None
    assert state.query_node_versions[0]["error"] == "query unavailable"
    assert state.storage_node_versions[0]["version"] == "24.8.14"
    assert state.storage_node_versions[0]["error"] is None


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_cluster_node_connection")
def test_get_cluster_state_falls_back_to_query_endpoint_when_topology_fails(
    get_ro_cluster_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    query_node = ClickhouseNode("query", 8123)
    cluster.is_single_node.return_value = False
    cluster.get_query_node.return_value = query_node
    cluster.get_distributed_nodes.side_effect = Exception("topology down")
    cluster.get_local_nodes.side_effect = Exception("topology down")

    def execute(sql: str, *args: object, **kwargs: object) -> mock.MagicMock:
        if "version()" in sql:
            return result([("25.3.6",)])
        return result([(["errors_local"],)])

    connection = mock.MagicMock()
    connection.execute.side_effect = execute
    get_ro_cluster_node_connection.return_value = connection

    state = _get_cluster_state(cluster)

    assert state.query_node_versions == [
        {"host": "query", "port": 8123, "version": "25.3.6", "error": None}
    ]
    assert state.storage_node_versions == []
    assert state.storage_node_error == "topology down"
    assert state.query_node_error is None
    assert state.tables == ["errors_local"]
    assert state.error is None
