from typing import Sequence

from snuba.clickhouse.columns import Column, UInt, String, DateTime
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

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
    Column("quantity", UInt(64)),
    Column("times_seen", UInt(64)),
]


class Migration(migration.ClickhouseNodeMigration):
    """
    Updates materialized view query to support category and quantity.
    Note that the consumer needs to be stopped for this migration.
    """

    blocking = True

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_mv_hourly_local",
            ),
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.OUTCOMES,
                view_name="outcomes_mv_hourly_local",
                destination_table_name="outcomes_hourly_local",
                columns=new_materialized_view_columns,
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
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
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

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
