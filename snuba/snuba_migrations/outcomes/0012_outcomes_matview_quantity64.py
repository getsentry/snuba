from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

hourly_materialized_view_columns: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("key_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("outcome", UInt(8)),
    Column("reason", String()),
    Column("category", UInt(8)),
    Column("quantity", UInt(64)),
    Column("times_seen", UInt(64)),
]

daily_materialized_view_columns: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("key_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("outcome", UInt(8)),
    Column("reason", String()),
    Column("category", UInt(8)),
    Column("quantity", UInt(64)),
    Column("times_seen", UInt(64)),
]


class Migration(migration.ClickhouseNodeMigration):
    """
    Updates the hourly and daily materialized views to aggregate quantity from the
    non-overflowing quantity64 raw column (sum(quantity64) AS quantity) instead of the
    legacy UInt32 quantity column. The destination tables are unchanged.

    Note that the consumer needs to be stopped for this migration.
    """

    blocking = True

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.OUTCOMES,
                view_name="outcomes_mv_hourly_local_v2",
                destination_table_name="outcomes_hourly_local",
                columns=hourly_materialized_view_columns,
                query="""
                    SELECT
                        org_id,
                        project_id,
                        ifNull(key_id, 0) AS key_id,
                        toStartOfHour(timestamp) AS timestamp,
                        outcome,
                        ifNull(reason, 'none') AS reason,
                        category,
                        count() AS times_seen,
                        sum(quantity64) AS quantity
                    FROM outcomes_raw_local
                    GROUP BY org_id, project_id, key_id, timestamp, outcome, reason, category
                """,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_mv_hourly_local",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.OUTCOMES,
                view_name="outcomes_mv_daily_local_v3",
                destination_table_name="outcomes_daily_local_v2",
                columns=daily_materialized_view_columns,
                query="""
                    SELECT
                        org_id,
                        project_id,
                        ifNull(key_id, 0) AS key_id,
                        toStartOfDay(timestamp) AS timestamp,
                        outcome,
                        ifNull(reason, 'none') AS reason,
                        category,
                        count() AS times_seen,
                        sum(quantity64) AS quantity
                    FROM outcomes_raw_local
                    GROUP BY org_id, project_id, key_id, timestamp, outcome, reason, category
                """,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_mv_daily_local_v2",
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_mv_hourly_local_v2",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.OUTCOMES,
                view_name="outcomes_mv_hourly_local",
                destination_table_name="outcomes_hourly_local",
                columns=hourly_materialized_view_columns,
                query="""
                    SELECT
                        org_id,
                        project_id,
                        ifNull(key_id, 0) AS key_id,
                        toStartOfHour(timestamp) AS timestamp,
                        outcome,
                        ifNull(reason, 'none') AS reason,
                        category,
                        count() AS times_seen,
                        sum(quantity) AS quantity
                    FROM outcomes_raw_local
                    GROUP BY org_id, project_id, key_id, timestamp, outcome, reason, category
                """,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_mv_daily_local_v3",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.OUTCOMES,
                view_name="outcomes_mv_daily_local_v2",
                destination_table_name="outcomes_daily_local_v2",
                columns=daily_materialized_view_columns,
                query="""
                    SELECT
                        org_id,
                        project_id,
                        ifNull(key_id, 0) AS key_id,
                        toStartOfDay(timestamp) AS timestamp,
                        outcome,
                        ifNull(reason, 'none') AS reason,
                        category,
                        count() AS times_seen,
                        sum(quantity) AS quantity
                    FROM outcomes_raw_local
                    GROUP BY org_id, project_id, key_id, timestamp, outcome, reason, category
                """,
                target=operations.OperationTarget.LOCAL,
            ),
        ]
