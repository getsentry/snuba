from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget

storage_set_name = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
local_table_name = "uptime_monitor_checks_v2_local"
dist_table_name = "uptime_monitor_checks_v2_dist"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=table_name,
                column=Column(
                    "incident_status",
                    UInt(16),
                ),
                target=target,
            )
            for (table_name, target) in [
                (local_table_name, OperationTarget.LOCAL),
                (dist_table_name, OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=table_name,
                column_name="incident_status",
                target=target,
            )
            for (table_name, target) in [
                (dist_table_name, OperationTarget.DISTRIBUTED),
                (local_table_name, OperationTarget.LOCAL),
            ]
        ]
