from typing import Sequence

from snuba.clickhouse.columns import Column, Float, Nested, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds the span_op_breakdowns nested column
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "span_op_breakdowns",
                    Nested(
                        [
                            ("key", String(Modifiers(low_cardinality=True))),
                            ("value", Float(64)),
                        ]
                    ),
                ),
                after="measurements.value",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_local", "span_op_breakdowns"
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column=Column(
                    "span_op_breakdowns",
                    Nested(
                        [
                            ("key", String(Modifiers(low_cardinality=True))),
                            ("value", Float(64)),
                        ]
                    ),
                ),
                after="measurements.value",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_dist", "span_op_breakdowns"
            )
        ]
