from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import AddIndicesData, OperationTarget, SqlOperation

storage_set_name = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
local_table_name = "eap_spans_local"
num_attr_buckets = 20

indices: Sequence[AddIndicesData] = [
    AddIndicesData(
        name=f"bf_attr_str_{i}",
        expression=f"mapKeys(attr_str_{i})",
        type="bloom_filter",
        granularity=1,
    )
    for i in range(num_attr_buckets)
] + [
    AddIndicesData(
        name=f"bf_attr_num_{i}",
        expression=f"mapKeys(attr_num_{i})",
        type="bloom_filter",
        granularity=1,
    )
    for i in range(num_attr_buckets)
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropIndices(
                storage_set=storage_set_name,
                table_name=local_table_name,
                indices=[idx.name for idx in indices],
                target=OperationTarget.LOCAL,
            ),
            operations.DropIndex(
                storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
                table_name="eap_spans_local",
                index_name="bf_project_id",
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddIndices(
                storage_set=storage_set_name,
                table_name=local_table_name,
                indices=indices,
                target=OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
                table_name="eap_spans_local",
                index_name="bf_project_id",
                index_expression="project_id",
                index_type="bloom_filter",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
        ]
