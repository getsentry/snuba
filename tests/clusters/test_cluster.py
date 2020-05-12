import importlib
from unittest.mock import patch

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters import cluster
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage


class TestClusters:
    def setup_class(self) -> None:
        settings.CLUSTERS = [
            {
                "host": "host_1",
                "port": 9000,
                "http_port": 8123,
                "storage_sets": {"events", "outcomes", "querylog", "sessions"},
                "distributed": False,
            },
            {
                "host": "host_2",
                "port": 9000,
                "http_port": 8123,
                "storage_sets": {"transactions"},
                "distributed": True,
                "cluster_name": "test_distributed_cluster",
            },
        ]
        importlib.reload(cluster)

    def teardown_class(self) -> None:
        importlib.reload(settings)
        importlib.reload(cluster)

    def test_clusters(self) -> None:
        assert (
            get_storage(StorageKey("events")).get_cluster()
            == get_storage(StorageKey("errors")).get_cluster()
        )

        assert (
            get_storage(StorageKey("events")).get_cluster()
            != get_storage(StorageKey("transactions")).get_cluster()
        )

    def test_get_storage_nodes(self) -> None:
        with patch.object(ClickhousePool, "execute") as execute:
            execute.return_value = [
                ("host_1", 9000, 1, 1),
                ("host_2", 9000, 2, 1),
            ]

            local_cluster = get_storage(StorageKey("events")).get_cluster()
            assert len(local_cluster.get_storage_nodes()) == 1
            assert local_cluster.get_storage_nodes()[0].host_name == "host_1"
            assert local_cluster.get_storage_nodes()[0].port == 9000
            assert local_cluster.get_storage_nodes()[0].shard is None
            assert local_cluster.get_storage_nodes()[0].replica is None

            distributed_cluster = get_storage(StorageKey("transactions")).get_cluster()
            assert len(distributed_cluster.get_storage_nodes()) == 2
            assert distributed_cluster.get_storage_nodes()[0].host_name == "host_1"
            assert distributed_cluster.get_storage_nodes()[1].host_name == "host_2"
