from typing import Sequence

from snuba.clickhouse.columns import (
    UUID,
    Column,
    DateTime,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from .matview import aggregate_columns_v1, create_matview_v1


raw_columns: Sequence[Column[Modifiers]] = [
    Column("session_id", UUID()),
    Column("distinct_id", UUID()),
    Column("seq", UInt(64)),
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("retention_days", UInt(16)),
    Column("duration", UInt(32)),
    Column("status", UInt(8)),
    Column("errors", UInt(16)),
    Column("received", DateTime()),
    Column("started", DateTime()),
    Column("release", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(low_cardinality=True))),
]


class Migration(migration.MultiStepMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_raw_local",
                columns=raw_columns,
                engine=table_engines.MergeTree(
                    storage_set=StorageSetKey.SESSIONS,
                    order_by="(org_id, project_id, release, environment, started)",
                    partition_by="(toMonday(started))",
                    settings={"index_granularity": "16384"},
                ),
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                columns=aggregate_columns_v1,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=StorageSetKey.SESSIONS,
                    order_by="(org_id, project_id, release, environment, started)",
                    partition_by="(toMonday(started))",
                    settings={"index_granularity": "256"},
                ),
            ),
            create_matview_v1,
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_mv_local",
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS, table_name="sessions_hourly_local",
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS, table_name="sessions_raw_local",
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_raw_dist",
                columns=raw_columns,
                engine=table_engines.Distributed(
                    local_table_name="sessions_raw_local", sharding_key="org_id",
                ),
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                columns=aggregate_columns_v1,
                engine=table_engines.Distributed(
                    local_table_name="sessions_hourly_local", sharding_key="org_id",
                ),
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS, table_name="sessions_hourly_dist",
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS, table_name="sessions_raw_dist",
            ),
        ]
