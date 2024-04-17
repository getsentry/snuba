from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    """
    This migration replaces discover_dist with discover_dist_new.
    """

    blocking = True
    old_table_name = "discover_dist"
    tmp_table_name = "discover_dist_temp"
    new_table_name = "discover_dist_new"

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.RenameTable(
                storage_set=StorageSetKey.DISCOVER,
                table_name=self.old_table_name,
                new_table_name=self.tmp_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.RenameTable(
                storage_set=StorageSetKey.DISCOVER,
                table_name=self.new_table_name,
                new_table_name=self.old_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.DISCOVER,
                table_name=self.tmp_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        # This is a destructive operation. We cannot rollback.
        return []
