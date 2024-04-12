from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations

table_name_prefix = "transactions"
column_name = "trace_id"


class Migration(migration.ClickhouseNodeMigration):
    """
    Add an index to trace_id
    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddIndex(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=f"{table_name_prefix}_local",
                index_name=f"bf_{column_name}",
                index_expression=column_name,
                index_type="bloom_filter(0.0)",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropIndex(
                StorageSetKey.TRANSACTIONS,
                table_name=f"{table_name_prefix}_local",
                index_name=f"bf_{column_name}",
                target=operations.OperationTarget.LOCAL,
            ),
        ]
