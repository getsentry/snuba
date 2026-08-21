from concurrent.futures import TimeoutError as FutureTimeoutError
from unittest import mock

from snuba.admin.clickhouse.clusters import _get_cluster_state, get_cluster_info
from snuba.clusters.cluster import ClickhouseCluster, ClickhouseNode


def result(rows: list[tuple[object, ...]]) -> mock.MagicMock:
    value = mock.MagicMock()
    value.results = rows
    return value


def test_get_cluster_state_queries_single_node_directly() -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    node = ClickhouseNode("single", 9000)
    cluster.is_single_node.return_value = True
    cluster.get_query_node.return_value = node
    cluster.get_query_connection.return_value.execute.return_value = result(
        [(["migrations_local"],)]
    )
    cluster.get_node_connection.return_value.execute.return_value = result([("25.3.6",)])

    state = _get_cluster_state(cluster)

    assert state.query_node_versions == [
        {"host": "single", "port": 9000, "version": "25.3.6", "error": None}
    ]
    assert state.storage_node_versions == state.query_node_versions
    assert state.tables == ["migrations_local"]


def test_get_cluster_state_separates_query_and_storage_node_versions() -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    query_node = ClickhouseNode("query", 9000)
    storage_node = ClickhouseNode("storage", 9001)
    cluster.is_single_node.return_value = False
    cluster.get_distributed_nodes.return_value = [query_node]
    cluster.get_local_nodes.return_value = [storage_node]
    cluster.get_query_connection.return_value.execute.return_value = result([([],)])
    cluster.get_node_connection.return_value.execute.side_effect = [
        result([("25.3.6",)]),
        result([("24.8.14",)]),
    ]

    state = _get_cluster_state(cluster)

    assert state.query_node_versions[0]["version"] == "25.3.6"
    assert state.query_node_versions[0]["host"] == "query"
    assert state.storage_node_versions[0]["version"] == "24.8.14"
    assert state.storage_node_versions[0]["host"] == "storage"


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


def test_get_cluster_state_isolates_node_version_errors() -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    query_node = ClickhouseNode("query", 9000)
    storage_node = ClickhouseNode("storage", 9001)
    cluster.is_single_node.return_value = False
    cluster.get_distributed_nodes.return_value = [query_node]
    cluster.get_local_nodes.return_value = [storage_node]
    cluster.get_query_connection.return_value.execute.return_value = result([([],)])
    cluster.get_node_connection.return_value.execute.side_effect = [
        Exception("query unavailable"),
        result([("24.8.14",)]),
    ]

    state = _get_cluster_state(cluster)

    assert state.query_node_versions[0]["version"] is None
    assert state.query_node_versions[0]["error"] == "query unavailable"
    assert state.storage_node_versions[0]["version"] == "24.8.14"
    assert state.storage_node_versions[0]["error"] is None
