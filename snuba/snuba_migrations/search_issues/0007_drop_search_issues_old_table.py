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
                table_name=params[0],
                target=params[1],
            )
            for params in [
                ("search_issues_local", OperationTarget.LOCAL),
                ("search_issues_dist", OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return []
