from dataclasses import dataclass
from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime64, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget


@dataclass(frozen=True)
class NewColumn:
    column: Column[Modifiers]
    after: str | None


new_columns: Sequence[NewColumn] = [
    NewColumn(
        column=Column(
            "start_timestamp",
            DateTime64(
                precision=6,
                modifiers=Modifiers(nullable=True, codecs=["DoubleDelta"]),
            ),
        ),
        after="timestamp",
    ),
    NewColumn(
        column=Column(
            "end_timestamp",
            DateTime64(
                precision=6,
                modifiers=Modifiers(nullable=True, codecs=["DoubleDelta"]),
            ),
        ),
        after="timestamp",
    ),
    NewColumn(column=Column("thread_id", String()), after=None),
    NewColumn(
        column=Column(
            "profiling_type",
            String(Modifiers(low_cardinality=True, default="'transaction'")),
        ),
        after=None,
    ),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    storage_set = StorageSetKey.FUNCTIONS

    local_raw_table = "functions_raw_local"
    dist_raw_table = "functions_raw_dist"

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=self.storage_set,
                table_name=table_name,
                column=new_column.column,
                after=new_column.after,
                target=target,
            )
            for table_name, target in [
                (self.local_raw_table, OperationTarget.LOCAL),
                (self.dist_raw_table, OperationTarget.DISTRIBUTED),
            ]
            for new_column in new_columns
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=self.storage_set,
                table_name=table_name,
                column_name=new_column.column.name,
                target=target,
            )
            for table_name, target in [
                (self.dist_raw_table, OperationTarget.DISTRIBUTED),
                (self.local_raw_table, OperationTarget.LOCAL),
            ]
            for new_column in new_columns
        ]
