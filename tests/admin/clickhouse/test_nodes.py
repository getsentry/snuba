from unittest import mock

from snuba.admin.clickhouse.nodes import _get_local_nodes
from snuba.clusters.cluster import _get_storage_set_cluster_map
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.storage_key import StorageKey

_OG_CLUSTER_MAP = _get_storage_set_cluster_map()


@mock.patch(
    "snuba.clusters.cluster._get_storage_set_cluster_map",
    return_value={StorageSetKey.EVENTS: _OG_CLUSTER_MAP[StorageSetKey.EVENTS]},
)
def test_get_local_nodes(map_mock: mock.MagicMock) -> None:
    assert _get_local_nodes(StorageKey.EVENTS) == []
    assert _get_local_nodes(StorageKey.TRANSACTIONS) == []
