from unittest import mock

from snuba.admin.clickhouse.nodes import _get_local_nodes
from snuba.clusters.cluster import _get_storage_set_cluster_map
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages import StorageKey

_OG_CLUSTER_MAP = _get_storage_set_cluster_map()


@mock.patch(
    "snuba.clusters.cluster._get_storage_set_cluster_map",
    return_value={StorageSetKey.ERRORS_V2: _OG_CLUSTER_MAP[StorageSetKey.ERRORS_V2]},
)
def test_get_local_nodes(map_mock: mock.MagicMock) -> None:
    assert _get_local_nodes(StorageKey.ERRORS_V2)
    assert _get_local_nodes(StorageKey.TRANSACTIONS) == []
