from snuba.admin.clickhouse.nodes import _get_dist_nodes, _get_local_nodes
from snuba.datasets.storages.storage_key import StorageKey


def test_get_local_and_distributed_nodes() -> None:
    # test we get the right nodes when dist and local have different hosts
    assert sorted(_get_local_nodes(StorageKey("errors")), key=lambda n: n["host"]) == [
        {"host": "clickhouse-02", "port": 9000},
        {"host": "clickhouse-03", "port": 9000},
    ]
    assert _get_dist_nodes(StorageKey("errors")) == [
        {"host": "clickhouse-query", "port": 9000}
    ]
