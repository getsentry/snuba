from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget, SqlOperation


class Migration(migration.ClickhouseNodeMigration):

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    local_table_name = "eap_trace_item_attrs_local"
    dist_table_name = "eap_trace_item_attrs_dist"
    mv_name = "eap_trace_item_attrs_mv"

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.mv_name,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return []
