from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    storage_set_key = StorageSetKey.GENERIC_METRICS_COUNTERS

    local_table_name = "generic_metric_counters_raw_local"
    dist_table_name = "generic_metric_counters_raw_dist"

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=self.storage_set_key,
                table_name=table_name,
                column=column,
                target=target,
            )
            for column in (
                Column(
                    "decasecond_retention_days",
                    UInt(8, MigrationModifiers(default=str("7"))),
                ),
                Column(
                    "min_retention_days",
                    UInt(8, MigrationModifiers(default=str("30"))),
                ),
            )
            for table_name, target in (
                (self.local_table_name, operations.OperationTarget.LOCAL),
                (self.dist_table_name, operations.OperationTarget.DISTRIBUTED),
            )
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=self.storage_set_key,
                table_name=table_name,
                column=column,
                target=target,
            )
            for column in (
                Column(
                    "decasecond_retention_days",
                    UInt(8, MigrationModifiers(default=str("retention_days"))),
                ),
                Column(
                    "min_retention_days",
                    UInt(8, MigrationModifiers(default=str("retention_days"))),
                ),
            )
            for table_name, target in (
                (self.dist_table_name, operations.OperationTarget.DISTRIBUTED),
                (self.local_table_name, operations.OperationTarget.LOCAL),
            )
        ]
