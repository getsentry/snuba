from unittest import mock

import pytest

from snuba.admin.clickhouse.nodes import _get_local_nodes
from snuba.clusters.cluster import _get_storage_set_cluster_map
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.storage_key import StorageKey

_OG_CLUSTER_MAP = _get_storage_set_cluster_map()


@mock.patch(
    "snuba.clusters.cluster._get_storage_set_cluster_map",
    return_value={StorageSetKey.EVENTS: _OG_CLUSTER_MAP[StorageSetKey.EVENTS]},
)
@pytest.mark.clickhouse_db
def test_get_local_nodes(map_mock: mock.MagicMock) -> None:
    """
    This test is checking that requesting a storage key not in the map doesn't cause any errors.
    """
    assert len(_get_local_nodes(StorageKey.ERRORS)) == 1
    assert _get_local_nodes(StorageKey.TRANSACTIONS) == []
