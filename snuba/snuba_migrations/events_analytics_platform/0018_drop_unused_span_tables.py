from __future__ import annotations

from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget, SqlOperation

unused_tables = {
    "eap_spans_local",
    "spans_num_attrs_2_local",
    "spans_num_attrs_2_mv",
    "spans_str_attrs_2_local",
    "spans_str_attrs_2_mv",
}


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=table_name,
                target=OperationTarget.LOCAL,
            )
            for table_name in unused_tables
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        pass
