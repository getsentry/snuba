from collections.abc import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations

table_name = "search_issues_local_v2"
column_name = "group_id"
index_name = "bf_group_id"


class Migration(migration.ClickhouseNodeMigration):
    """Add a bloom filter index to group_id on the search_issues storage."""

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddIndex(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name=table_name,
                index_name=index_name,
                index_expression=column_name,
                index_type="bloom_filter",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropIndex(
                StorageSetKey.SEARCH_ISSUES,
                table_name=table_name,
                index_name=index_name,
                target=operations.OperationTarget.LOCAL,
            ),
        ]
