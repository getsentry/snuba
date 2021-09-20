from typing import Sequence

from snuba.clickhouse.columns import Column, Float, Nested, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "spans",
                    Nested(
                        [
                            ("op", String(Modifiers(low_cardinality=True))),
                            ("group", UInt(64)),
                            ("exclusive_time", Float(64)),
                        ]
                    ),
                ),
                after="span_op_breakdowns.value",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_local", "spans"
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column=Column(
                    "spans",
                    Nested(
                        [
                            ("op", String(Modifiers(low_cardinality=True))),
                            ("group", UInt(64)),
                            ("exclusive_time", Float(64)),
                        ]
                    ),
                ),
                after="span_op_breakdowns.value",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_dist", "spans"
            )
        ]
