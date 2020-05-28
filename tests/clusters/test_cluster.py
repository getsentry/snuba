from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseCluster


def test_cache_connections() -> None:
    cluster = ClickhouseCluster("localhost", 8000, 8001, {"events"}, True)

    assert cluster.get_connection(
        ClickhouseClientSettings.QUERY
    ) == cluster.get_connection(ClickhouseClientSettings.QUERY)
