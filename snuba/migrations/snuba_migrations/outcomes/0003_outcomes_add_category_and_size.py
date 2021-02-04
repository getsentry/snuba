from typing import Sequence

from snuba.clickhouse.columns import Column, UInt, String, DateTime
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

# TODO: materialized view migration
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
    Column("category", UInt(8)),
    Column("size", UInt(32)),
    Column("times_seen", UInt(64)),
]


class Migration(migration.MultiStepMigration):
    """
    Adds the http columns defined, with the method and referer coming from the request interface
    and url materialized from the tags.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_local",
                column=Column("size", UInt(32, Modifiers(nullable=True))),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_local",
                column=Column("category", UInt(8)),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_local",
                column=Column("size", UInt(32)),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_local",
                column=Column("category", UInt(8)),
                after=None,
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
                        ifNull(size, 0) AS size,
                        ifNull(category, 1) AS category,
                        count() AS times_seen
                    FROM outcomes_raw_local
                    GROUP BY org_id, project_id, key_id, timestamp, outcome, reason, size, category
                """,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(StorageSetKey.OUTCOMES, "outcomes_raw_local", "size"),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_raw_local", "category"
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_local", "size"
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_local", "category"
            ),
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_mv_hourly_local",
            ),
            # TODO: put old mat view query in own file, use reference to that?
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
                column=Column("size", UInt(32, Modifiers(nullable=True))),
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
                column=Column("size", UInt(32)),
                after=None,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_dist",
                column=Column("category", UInt(8)),
                after=None,
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(StorageSetKey.OUTCOMES, "outcomes_raw_dist", "size"),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_raw_dist", "category"
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_dist", "size"
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_dist", "category"
            ),
        ]
