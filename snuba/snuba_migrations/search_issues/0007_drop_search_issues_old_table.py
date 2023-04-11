from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget, SqlOperation


class Migration(migration.ClickhouseNodeMigration):
    """
    Drops the original 'v1' table in favor of the 'v2' table.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_dist",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_local",
                target=OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return []
