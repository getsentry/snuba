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


def test_column_based_partition_selector() -> None:
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
    assert cluster is not None
