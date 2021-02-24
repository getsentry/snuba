from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

UNKNOWN_SPAN_STATUS = 2


class Migration(migration.ClickhouseNodeMigration):
    """
    The second of two migrations that syncs the transactions_local table for onpremise
    users migrating from versions of Snuba prior to the migration system.

    This migration ensures the list of columns is up to date.
    """

    blocking = True  # Just to be safe since we are changing some column types

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("duration", UInt(32)),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "sdk_name", String(Modifiers(low_cardinality=True, default="''"))
                ),
                after="user_email",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "sdk_version", String(Modifiers(low_cardinality=True, default="''"))
                ),
                after="sdk_name",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "transaction_status",
                    UInt(8, Modifiers(default=str(UNKNOWN_SPAN_STATUS))),
                ),
                after="transaction_op",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("_tags_flattened", String()),
                after="tags",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("_contexts_flattened", String()),
                after="contexts",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "user_hash", UInt(64, Modifiers(materialized="cityHash64(user)"))
                ),
                after="user",
            ),
            # The following columns were originally created as non low cardinality strings
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "transaction_name", String(Modifiers(low_cardinality=True))
                ),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "release", String(Modifiers(nullable=True, low_cardinality=True))
                ),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "dist", String(Modifiers(nullable=True, low_cardinality=True))
                ),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "sdk_name", String(Modifiers(low_cardinality=True, default="''"))
                ),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "sdk_version", String(Modifiers(low_cardinality=True, default="''"))
                ),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "environment",
                    String(Modifiers(nullable=True, low_cardinality=True)),
                ),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("message_timestamp", DateTime()),
                after="offset",
            ),
            operations.DropColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column_name="_start_date",
            ),
            operations.DropColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column_name="_finish_date",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
