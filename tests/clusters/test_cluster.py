from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseCluster


def test_cache_connections() -> None:
    cluster = ClickhouseCluster("localhost", 8000, 8001, {"events"})

    assert cluster.get_connection(
        ClickhouseClientSettings.READONLY
    ) == cluster.get_connection(ClickhouseClientSettings.READONLY)

    assert cluster.get_connection(
        ClickhouseClientSettings.REPLACER, 5
    ) == cluster.get_connection(ClickhouseClientSettings.REPLACER, 5)
