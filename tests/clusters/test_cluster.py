from snuba import settings
from unittest.mock import patch

settings.CLUSTERS = [
    {
        "host": settings.CLICKHOUSE_HOST,
        "port": settings.CLICKHOUSE_PORT,
        "http_port": settings.CLICKHOUSE_HTTP_PORT,
        "storage_sets": {"events", "outcomes", "querylog", "sessions"},
        "distributed": False,
    },
    {
        "host": settings.CLICKHOUSE_HOST,
        "port": settings.CLICKHOUSE_PORT,
        "http_port": settings.CLICKHOUSE_HTTP_PORT,
        "storage_sets": {"transactions"},
        "distributed": True,
        "cluster_name": "test_distributed_cluster",
    },
]


class TestClusters:
    def test_clusters(self) -> None:
        from snuba.datasets.storages import StorageKey
        from snuba.datasets.storages.factory import get_storage

        assert (
            get_storage(StorageKey("events")).get_cluster()
            == get_storage(StorageKey("errors")).get_cluster()
        )

        assert (
            get_storage(StorageKey("events")).get_cluster()
            != get_storage(StorageKey("transactions")).get_cluster()
        )

    def test_get_nodes(self) -> None:
        from snuba.clickhouse.native import ClickhousePool
        from snuba.datasets.storages import StorageKey
        from snuba.datasets.storages.factory import get_storage

        with patch.object(ClickhousePool, "execute") as execute:
            execute.return_value = [
                ("host_1", 9000, 1, 1),
                ("host_2", 9000, 2, 1),
            ]

            local_cluster = get_storage(StorageKey("events")).get_cluster()
            assert len(local_cluster.get_nodes()) == 1
            assert local_cluster.get_nodes()[0].host_name == settings.CLICKHOUSE_HOST
            assert local_cluster.get_nodes()[0].port == settings.CLICKHOUSE_PORT
            assert local_cluster.get_nodes()[0].shard is None
            assert local_cluster.get_nodes()[0].replica is None

            distributed_cluster = get_storage(StorageKey("transactions")).get_cluster()
            assert len(distributed_cluster.get_nodes()) == 2
            assert distributed_cluster.get_nodes()[0].host_name == "host_1"
            assert distributed_cluster.get_nodes()[1].host_name == "host_2"
