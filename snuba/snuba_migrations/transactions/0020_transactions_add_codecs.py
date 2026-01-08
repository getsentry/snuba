from typing import Sequence

from snuba.clickhouse.columns import UUID, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    """
    This migrations adds the codecs to columns to match what is in SaaS.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            # Modifying the codec of a key column is not supported in Clickhouse 20.
            # So we dont add codecs to the key columns span_id, and event_id.
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("trace_id", UUID(Modifiers(nullable=True, codecs=["LZ4"]))),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("offset", UInt(64, Modifiers(codecs=["T64", "LZ4"]))),
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            # Clickhouse 20 doesn't support resetting the codec to the default.
            # So these are no-ops that can become effective when we upgrade to 21.
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("trace_id", UUID(Modifiers(nullable=True))),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("offset", UInt(64)),
                target=operations.OperationTarget.LOCAL,
            ),
        ]
