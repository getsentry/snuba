from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    local_table_name = "eap_items_1_local"
    dist_table_name = "eap_items_1_dist"
    column_to_remove = "hashed_keys"

    def forwards_ops(self) -> list[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column_name=self.column_to_remove,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column_name=self.column_to_remove,
                target=OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> list[operations.SqlOperation]:
        return []
