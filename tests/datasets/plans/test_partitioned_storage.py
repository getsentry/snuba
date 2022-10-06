from unittest.mock import patch

from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.partitioned_storage import ColumnBasedStoragePartitionSelector
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import HTTPQuerySettings

DISTS_ENTITY_KEY = EntityKey("generic_metrics_distributions")
DISTS_STORAGE_KEY = StorageKey("generic_metrics_distributions")
DISTS_STORAGE_SET_KEY = StorageSetKey("generic_metrics_distributions")

"""
This mock logical partition : slice mapping should follow the pattern of
0: 0
1: 1
2: 0
3: 1
...
"""
MOCK_LOGICAL_PART_MAPPING = {
    "generic_metrics_distributions": {x: x % 2 for x in range(0, 256)}
}

SLICE_0_DATABASE_VALUE = "slice_0_db"
SLICE_1_DATABASE_VALUE = "slice_1_db"

SLICED_CLUSTERS_CONFIG = [
    {
        "host": "host_slice",
        "port": 9000,
        "user": "default",
        "password": "",
        "database": SLICE_0_DATABASE_VALUE,
        "http_port": 8123,
        "storage_set_slices": {("generic_metrics_distributions", 0)},
        "single_node": True,
    },
    {
        "host": "host_slice",
        "port": 9001,
        "user": "default",
        "password": "",
        "database": SLICE_1_DATABASE_VALUE,
        "http_port": 8124,
        "storage_set_slices": {("generic_metrics_distributions", 1)},
        "single_node": True,
    },
]


@patch("snuba.settings.SLICED_STORAGES", {"generic_metrics_distributions": 2})
@patch("snuba.settings.LOGICAL_PARTITION_MAPPING", MOCK_LOGICAL_PART_MAPPING)
@patch("snuba.settings.SLICED_CLUSTERS", SLICED_CLUSTERS_CONFIG)
def test_column_based_partition_selector_slice_1() -> None:
    """
    Tests that the column based partition selector selects the right cluster
    for a query.
    """
    query = LogicalQuery(
        QueryEntity(
            DISTS_ENTITY_KEY,
            get_entity(DISTS_ENTITY_KEY).get_data_model(),
        ),
        selected_columns=[],
        condition=binary_condition(
            "equals",
            Column("_snuba_org_id", None, "org_id"),
            Literal(None, 1),
        ),
    )

    settings = HTTPQuerySettings()
    selector = ColumnBasedStoragePartitionSelector(
        DISTS_STORAGE_KEY,
        DISTS_STORAGE_SET_KEY,
        "org_id",
    )
    cluster = selector.select_cluster(query, settings)

    # org_id 1 should be assigned to slice 1 because (1 % 256 == logical partition 1) and
    # all odd logical partitions are assigned to slice 1
    assert cluster.get_database() == SLICE_1_DATABASE_VALUE


@patch("snuba.settings.SLICED_STORAGES", {"generic_metrics_distributions": 2})
@patch("snuba.settings.LOGICAL_PARTITION_MAPPING", MOCK_LOGICAL_PART_MAPPING)
@patch("snuba.settings.SLICED_CLUSTERS", SLICED_CLUSTERS_CONFIG)
def test_column_based_partition_selector_slice_0() -> None:
    """
    Tests that the column based partition selector selects the right cluster
    for a query.
    """
    query = LogicalQuery(
        QueryEntity(
            DISTS_ENTITY_KEY,
            get_entity(DISTS_ENTITY_KEY).get_data_model(),
        ),
        selected_columns=[],
        condition=binary_condition(
            "equals",
            Column("_snuba_org_id", None, "org_id"),
            Literal(None, 500),
        ),
    )

    settings = HTTPQuerySettings()
    selector = ColumnBasedStoragePartitionSelector(
        DISTS_STORAGE_KEY,
        DISTS_STORAGE_SET_KEY,
        "org_id",
    )
    cluster = selector.select_cluster(query, settings)

    # org_id 500 should be assigned to slice 0 because (500 % 256 == logical partition 244) and
    # all even logical partitions are assigned to slice 0
    assert cluster.get_database() == SLICE_0_DATABASE_VALUE
