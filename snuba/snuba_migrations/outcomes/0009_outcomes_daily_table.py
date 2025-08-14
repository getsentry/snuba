from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

daily_columns: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("key_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("category", UInt(8)),
    Column("outcome", UInt(8)),
    Column("reason", String(Modifiers(low_cardinality=True))),
    Column("quantity", UInt(64)),
    Column("times_seen", UInt(64)),
]

materialized_view_columns: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("key_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("category", UInt(8)),
    Column("outcome", UInt(8)),
    Column("reason", String()),
    Column("quantity", UInt(64)),
    Column("times_seen", UInt(64)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_daily_local",
                columns=daily_columns,
                engine=table_engines.SummingMergeTree(
                    storage_set=StorageSetKey.OUTCOMES,
                    order_by="(org_id, project_id, key_id, outcome, reason, timestamp, category)",
                    partition_by="(toMonth(timestamp))",
                    ttl="timestamp + toIntervalMonth(13)",
                ),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.OUTCOMES,
                view_name="outcomes_mv_daily_local",
                destination_table_name="outcomes_daily_local",
                columns=materialized_view_columns,
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
            operations.CreateTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_daily_dist",
                columns=daily_columns,
                engine=table_engines.Distributed(
                    local_table_name="outcomes_daily_local",
                    sharding_key="org_id",
                ),
                target=operations.OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_daily_dist",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_mv_daily_local",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_daily_local",
                target=operations.OperationTarget.LOCAL,
            ),
        ]
