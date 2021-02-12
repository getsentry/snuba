from typing import Sequence

from snuba.clickhouse.columns import Column, UInt, String, DateTime
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from sentry_relay import DataCategory

old_materialized_view_columns: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("key_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("outcome", UInt(8)),
    Column("reason", String()),
    Column("times_seen", UInt(64)),
]

new_materialized_view_columns: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("key_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("outcome", UInt(8)),
    Column("reason", String()),
    Column("category", UInt(8, Modifiers(nullable=True))),
    Column("quantity", UInt(64, Modifiers(nullable=True))),
    Column("times_seen", UInt(64)),
]


class Migration(migration.MultiStepMigration):
    """
    Adds quantity and category columns to outcomes. updates materialized view and hourly table to support
    category as a new dimension and quantity as a new measure.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_local",
                column=Column("quantity", UInt(32, Modifiers(nullable=True))),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_local",
                column=Column("category", UInt(8, Modifiers(nullable=True))),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_local",
                column=Column("quantity", UInt(64, Modifiers(nullable=True))),
                after=None,
            ),
            operations.RunSql(
                storage_set=StorageSetKey.OUTCOMES,
                statement="""
                    ALTER TABLE outcomes_hourly_local ADD COLUMN IF NOT EXISTS category UInt8,
                    MODIFY ORDER BY (org_id, project_id, key_id, outcome, reason, timestamp, category);
                """,
            ),  # note: this migration is not reversable!
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_mv_hourly_local",
            ),
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.OUTCOMES,
                view_name="outcomes_mv_hourly_local",
                destination_table_name="outcomes_hourly_local",
                columns=new_materialized_view_columns,
                query=f"""
                    SELECT
                        org_id,
                        project_id,
                        ifNull(key_id, 0) AS key_id,
                        toStartOfHour(timestamp) AS timestamp,
                        outcome,
                        ifNull(reason, 'none') AS reason,
                        ifNull(category,{DataCategory.ERROR}) as category,
                        count() AS times_seen,
                        sum(quantity) AS quantity
                    FROM outcomes_raw_local
                    GROUP BY org_id, project_id, key_id, timestamp, outcome, reason, category
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
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_mv_hourly_local",
            ),
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.OUTCOMES,
                view_name="outcomes_mv_hourly_local",
                destination_table_name="outcomes_hourly_local",
                columns=old_materialized_view_columns,
                query="""
                    SELECT
                        org_id,
                        project_id,
                        ifNull(key_id, 0) AS key_id,
                        toStartOfHour(timestamp) AS timestamp,
                        outcome,
                        ifNull(reason, 'none') AS reason,
                        count() AS times_seen
                    FROM outcomes_raw_local
                    GROUP BY org_id, project_id, key_id, timestamp, outcome, reason
                """,
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
                column=Column("category", UInt(8, Modifiers(nullable=True))),
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
                """,  # note: this migration is not reversable!
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
