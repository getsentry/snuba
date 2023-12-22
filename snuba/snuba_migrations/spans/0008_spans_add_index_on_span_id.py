from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations

table_name_prefix = "spans"
column_name = "span_id"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddIndex(
                storage_set=StorageSetKey.SPANS,
                table_name=f"{table_name_prefix}_local",
                index_name=f"bf_{column_name}",
                index_expression=column_name,
                index_type="bloom_filter()",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.SPANS,
                table_name=f"{table_name_prefix}_dist",
                index_name=f"bf_{column_name}",
                index_expression=column_name,
                index_type="bloom_filter()",
                granularity=1,
                target=operations.OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropIndex(
                StorageSetKey.SPANS,
                table_name=f"{table_name_prefix}_local",
                index_name=f"bf_{column_name}",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropIndex(
                StorageSetKey.SPANS,
                table_name=f"{table_name_prefix}_dist",
                index_name=f"bf_{column_name}",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
        ]
