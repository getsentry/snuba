from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.operations import OperationTarget, SqlOperation


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    storage_set_key = StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS

    local_table_name = "generic_metric_distributions_raw_local"
    dist_table_name = "generic_metric_distributions_raw_dist"

    columns = [
        (
            Column("enable_histogram", UInt(8, MigrationModifiers(default=str("0")))),
            "granularities",
        ),
        (
            Column(
                "decasecond_retention_days",
                UInt(8, MigrationModifiers(default=str("retention_days"))),
            ),
            "enable_histogram",
        ),
        (
            Column(
                "min_retention_days",
                UInt(8, MigrationModifiers(default=str("retention_days"))),
            ),
            "decasecond_retention_days",
        ),
        (
            Column(
                "hr_retention_days",
                UInt(8, MigrationModifiers(default=str("retention_days"))),
            ),
            "min_retention_days",
        ),
        (
            Column(
                "day_retention_days",
                UInt(8, MigrationModifiers(default=str("retention_days"))),
            ),
            "hr_retention_days",
        ),
    ]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=table_name,
                column=column,
                after=after,
                target=target,
            )
            for column, after in self.columns
            for table_name, target in [
                (self.local_table_name, OperationTarget.LOCAL),
                (self.dist_table_name, OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=table_name,
                column_name=column.name,
                target=target,
            )
            for column, _ in self.columns
            for table_name, target in [
                (self.dist_table_name, OperationTarget.DISTRIBUTED),
                (self.local_table_name, OperationTarget.LOCAL),
            ]
        ]
