from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.MultiStepMigration):
    """
    Adds quantity and category columns to outcomes. updates hourly table to support
    category as a new dimension and quantity as a new measure.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_local",
                column=Column("quantity", UInt(32, Modifiers(nullable=True))),
                after="reason",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_local",
                column=Column("category", UInt(8)),
                after="timestamp",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_local",
                column=Column("quantity", UInt(64, Modifiers(nullable=True))),
                after="reason",
            ),
            operations.RunSql(
                storage_set=StorageSetKey.OUTCOMES,
                statement="""
                    ALTER TABLE outcomes_hourly_local ADD COLUMN IF NOT EXISTS category UInt8 AFTER timestamp,
                    MODIFY ORDER BY (org_id, project_id, key_id, outcome, reason, timestamp, category);
                """,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_raw_local", "quantity"
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_raw_local", "category"
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_local", "quantity"
            ),
            operations.RunSql(
                storage_set=StorageSetKey.OUTCOMES,
                statement="""
                    ALTER TABLE outcomes_hourly_local
                    MODIFY ORDER BY (org_id, project_id, key_id, outcome, reason, timestamp);
                """,
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_local", "category"
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_dist",
                column=Column("quantity", UInt(32, Modifiers(nullable=True))),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_dist",
                column=Column("category", UInt(8)),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_dist",
                column=Column("quantity", UInt(64, Modifiers(nullable=True))),
                after=None,
            ),
            operations.RunSql(
                storage_set=StorageSetKey.OUTCOMES,
                statement="""
                    ALTER TABLE outcomes_hourly_local ADD COLUMN IF NOT EXISTS category UInt8,
                    MODIFY ORDER BY (org_id, project_id, key_id, outcome, reason, timestamp, category);
                """,
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_raw_dist", "quantity"
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_raw_dist", "category"
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_dist", "quantity"
            ),
            operations.RunSql(
                storage_set=StorageSetKey.OUTCOMES,
                statement="""
                    ALTER TABLE outcomes_hourly_local
                    MODIFY ORDER BY (org_id, project_id, key_id, outcome, reason, timestamp);
                """,
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_local", "category"
            ),
        ]
