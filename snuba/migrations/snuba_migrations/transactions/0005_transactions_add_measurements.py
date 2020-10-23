from typing import Sequence

from snuba.clickhouse.columns import Column, Float, Nested, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import lowcardinality


class Migration(migration.MultiStepMigration):
    """
    Adds the measurements nested column
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "measurements",
                    Nested([("key", String(lowcardinality())), ("value", Float(64))]),
                ),
                after="_contexts_flattened",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_local", "measurements"
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column=Column(
                    "measurements",
                    Nested([("key", String(lowcardinality())), ("value", Float(64))]),
                ),
                after="_contexts_flattened",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_dist", "measurements"
            )
        ]
