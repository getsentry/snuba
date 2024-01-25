from typing import List, Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set_name = StorageSetKey.METRICS_SUMMARIES

columns: List[Column[Modifiers]] = [
    Column("segment_id", UInt(64)),
    Column("duration_ms", UInt(32)),
    Column("group", UInt(64)),
    Column("is_segment", UInt(8)),
]

targets = [
    (
        OperationTarget.LOCAL,
        "metrics_summaries_local",
    ),
    (
        OperationTarget.DISTRIBUTED,
        "metrics_summaries_dist",
    ),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=table_name,
                column=column,
                target=target,
            )
            for column in columns
            for table_name, target in targets
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=table_name,
                column_name=column.name,
                target=target,
            )
            for column in columns
            for target, table_name in targets
        ]
