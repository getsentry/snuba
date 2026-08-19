from snuba.admin.clickhouse.nodes import _get_dist_nodes, _get_local_nodes
from snuba.datasets.storages.storage_key import StorageKey


def test_get_local_and_distributed_nodes() -> None:
    # Admin lists the connect port (8123 on replicas), not the TCP port.
    assert sorted(_get_local_nodes(StorageKey("errors")), key=lambda n: n["host"]) == [
        {"host": "clickhouse-02", "port": 8123},
        {"host": "clickhouse-03", "port": 8123},
    ]
    assert _get_dist_nodes(StorageKey("errors")) == [{"host": "clickhouse-query", "port": 8123}]
