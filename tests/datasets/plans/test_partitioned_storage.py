from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.partitioned_storage import ColumnBasedStoragePartitionSelector
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import HTTPQuerySettings


def test_column_based_partition_selector() -> None:
    """
    Tests that the column based partition selector selects the right cluster
    for a query.
    """
    query = LogicalQuery(
        QueryEntity(
            EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
            get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
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
        StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS, "org_id"
    )
    cluster = selector.select_cluster(query, settings)
    assert cluster is not None
