from typing import Sequence

from snuba.clickhouse.columns import (
    AggregateFunction,
    Column,
    DateTime,
    String,
    UInt,
    UUID,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import lowcardinality
from snuba.processor import MAX_UINT32, NIL_UUID


raw_columns = [
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
    Column("release", String(lowcardinality())),
    Column("environment", String(lowcardinality())),
]


aggregate_columns = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("started", DateTime()),
    Column("release", String(lowcardinality())),
    Column("environment", String(lowcardinality())),
    Column(
        "duration_quantiles",
        AggregateFunction("quantilesIf(0.5, 0.9)", UInt(32), UInt(8)),
    ),
    Column("sessions", AggregateFunction("countIf", UUID(), UInt(8))),
    Column("users", AggregateFunction("uniqIf", UUID(), UInt(8))),
    Column("sessions_crashed", AggregateFunction("countIf", UUID(), UInt(8)),),
    Column("sessions_abnormal", AggregateFunction("countIf", UUID(), UInt(8)),),
    Column("sessions_errored", AggregateFunction("uniqIf", UUID(), UInt(8))),
    Column("users_crashed", AggregateFunction("uniqIf", UUID(), UInt(8))),
    Column("users_abnormal", AggregateFunction("uniqIf", UUID(), UInt(8))),
    Column("users_errored", AggregateFunction("uniqIf", UUID(), UInt(8))),
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
                columns=aggregate_columns,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=StorageSetKey.SESSIONS,
                    order_by="(org_id, project_id, release, environment, started)",
                    partition_by="(toMonday(started))",
                    settings={"index_granularity": "256"},
                ),
            ),
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.SESSIONS,
                view_name="sessions_hourly_mv_local",
                destination_table_name="sessions_hourly_local",
                columns=aggregate_columns,
                query=f"""
                    SELECT
                        org_id,
                        project_id,
                        toStartOfHour(started) as started,
                        release,
                        environment,
                        quantilesIfState(0.5, 0.9)(
                            duration,
                            duration <> {MAX_UINT32} AND status == 1
                        ) as duration_quantiles,
                        countIfState(session_id, seq == 0) as sessions,
                        uniqIfState(distinct_id, distinct_id != '{NIL_UUID}') as users,
                        countIfState(session_id, status == 2) as sessions_crashed,
                        countIfState(session_id, status == 3) as sessions_abnormal,
                        uniqIfState(session_id, errors > 0) as sessions_errored,
                        uniqIfState(distinct_id, status == 2) as users_crashed,
                        uniqIfState(distinct_id, status == 3) as users_abnormal,
                        uniqIfState(distinct_id, errors > 0) as users_errored
                    FROM
                        sessions_raw_local
                    GROUP BY
                        org_id, project_id, started, release, environment
                """,
            ),
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
                columns=aggregate_columns,
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
