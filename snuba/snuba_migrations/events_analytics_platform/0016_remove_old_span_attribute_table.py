from __future__ import annotations

from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.operations import DropTable, OperationTarget, SqlOperation


class Migration(ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    meta_local_table_name = "spans_attributes_meta_local"
    meta_dist_table_name = "spans_attributes_meta_dist"

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            DropTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_local_table_name,
                target=OperationTarget.LOCAL,
            ),
            DropTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return []  # All or nothing
