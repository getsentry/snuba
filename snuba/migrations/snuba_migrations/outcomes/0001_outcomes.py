from typing import Sequence
from snuba.clickhouse.columns import (
    Column,
    DateTime,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import LowCardinality


raw_columns = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("key_id", UInt(64, [Nullable()])),
    Column("timestamp", DateTime()),
    Column("outcome", UInt(8)),
    Column("reason", String([Nullable(), LowCardinality()])),
    Column("event_id", UUID([Nullable()])),
]

hourly_columns = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("key_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("outcome", UInt(8)),
    Column("reason", String([LowCardinality()])),
    Column("times_seen", UInt(64)),
]

materialized_view_columns = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("key_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("outcome", UInt(8)),
    Column("reason", String()),
    Column("times_seen", UInt(64)),
]


class Migration(migration.MultiStepMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_local",
                columns=raw_columns,
                engine=table_engines.MergeTree(
                    storage_set=StorageSetKey.OUTCOMES,
                    order_by="(org_id, project_id, timestamp)",
                    partition_by="(toMonday(timestamp))",
                    settings={"index_granularity": "16384"},
                ),
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_local",
                columns=hourly_columns,
                engine=table_engines.SummingMergeTree(
                    storage_set=StorageSetKey.OUTCOMES,
                    order_by="(org_id, project_id, key_id, outcome, reason, timestamp)",
                    partition_by="(toMonday(timestamp))",
                    settings={"index_granularity": "256"},
                ),
            ),
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.OUTCOMES,
                view_name="outcomes_mv_hourly_local",
                destination_table_name="outcomes_hourly_local",
                columns=materialized_view_columns,
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

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_mv_hourly_local",
            ),
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES, table_name="outcomes_hourly_local",
            ),
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES, table_name="outcomes_raw_local",
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_dist",
                columns=raw_columns,
                engine=table_engines.Distributed(
                    local_table_name="outcomes_raw_local", sharding_key="org_id",
                ),
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_dist",
                columns=hourly_columns,
                engine=table_engines.Distributed(
                    local_table_name="outcomes_hourly_local", sharding_key="org_id",
                ),
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES, table_name="outcomes_hourly_local"
            ),
            operations.DropTable(
                storage_set=StorageSetKey.OUTCOMES, table_name="outcomes_raw_dist",
            ),
        ]
