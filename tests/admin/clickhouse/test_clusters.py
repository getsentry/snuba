from unittest import mock

from snuba.admin.clickhouse.clusters import _get_cluster_state
from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseCluster


def test_get_cluster_state_without_registered_storage() -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    connection = cluster.get_query_connection.return_value
    connection.execute.return_value.results = [("25.3.6", ["migrations_local"])]

    state = _get_cluster_state(cluster, None)

    cluster.get_query_connection.assert_called_once_with(ClickhouseClientSettings.QUERY)
    assert state.version == "25.3.6"
    assert state.tables == ["migrations_local"]


@mock.patch("snuba.admin.clickhouse.clusters.get_ro_query_node_connection")
def test_get_cluster_state_with_registered_storage(
    get_ro_query_node_connection: mock.MagicMock,
) -> None:
    cluster = mock.MagicMock(spec=ClickhouseCluster)
    connection = get_ro_query_node_connection.return_value
    connection.execute.return_value.results = [("25.3.6", [])]

    state = _get_cluster_state(cluster, "errors")

    get_ro_query_node_connection.assert_called_once_with("errors", ClickhouseClientSettings.QUERY)
    cluster.get_query_connection.assert_not_called()
    assert state.version == "25.3.6"
    assert state.tables == []
