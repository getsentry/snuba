import os

from snuba.admin.clickhouse.nodes import _get_dist_nodes, _get_local_nodes
from snuba.datasets.storages.storage_key import StorageKey


def test_get_local_and_distributed_nodes() -> None:
    # The default layout has two storage shards. The single-shard cells of the
    # clickhouse-mixed-versions CI job set EXPECTED_STORAGE_HOSTS=clickhouse-02.
    storage_hosts = os.environ.get("EXPECTED_STORAGE_HOSTS") or "clickhouse-02,clickhouse-03"
    # Admin lists the connect port (8123 on replicas), not the TCP port.
    assert sorted(_get_local_nodes(StorageKey("errors")), key=lambda n: n["host"]) == [
        {"host": host, "port": 8123} for host in sorted(storage_hosts.split(","))
    ]
    assert _get_dist_nodes(StorageKey("errors")) == [{"host": "clickhouse-query", "port": 8123}]
