from dataclasses import dataclass
from typing import Sequence

from snuba.clickhouse.columns import Column, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget


@dataclass(frozen=True)
class NewColumn:
    column: Column[Modifiers]
    after: str


UNKNOWN_SPAN_STATUS = 2

new_columns: Sequence[NewColumn] = [
    NewColumn(column=Column("function", String()), after="name"),
    NewColumn(column=Column("module", String()), after="package"),
    NewColumn(
        column=Column("dist", String(Modifiers(nullable=True, low_cardinality=True))),
        after="release",
    ),
    NewColumn(
        column=Column("transaction_op", String(Modifiers(low_cardinality=True))),
        after="dist",
    ),
    NewColumn(
        column=Column(
            "transaction_status", UInt(8, Modifiers(default=str(UNKNOWN_SPAN_STATUS)))
        ),
        after="transaction_op",
    ),
    NewColumn(
        column=Column(
            "http_method", String(Modifiers(nullable=True, low_cardinality=True))
        ),
        after="transaction_status",
    ),
    NewColumn(
        column=Column(
            "browser_name", String(Modifiers(nullable=True, low_cardinality=True))
        ),
        after="http_method",
    ),
    NewColumn(
        column=Column("device_classification", UInt(8)),
        after="browser_name",
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
