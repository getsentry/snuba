from typing import Sequence

from snuba.clickhouse.columns import (
    UUID,
    AggregateFunction,
    Column,
    DateTime,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.processor import MAX_UINT32, NIL_UUID
from .matview import create_matview_v1


aggregate_columns: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("started", DateTime()),
    Column("release", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(low_cardinality=True))),
    Column("user_agent", String(Modifiers(low_cardinality=True))),
    Column("os", String(Modifiers(low_cardinality=True))),
    # durations
    Column(
        "duration_quantiles",
        AggregateFunction("quantilesIf(0.5, 0.9)", [UInt(32), UInt(8)]),
    ),
    Column("duration_avg", AggregateFunction("avgIf", [UInt(32), UInt(8)])),
    # sessions:
    Column("sessions", AggregateFunction("countIf", [UUID(), UInt(8)])),
    Column("sessions_preaggr", AggregateFunction("sumIf", [UInt(32), UInt(8)])),
    Column("sessions_crashed", AggregateFunction("countIf", [UUID(), UInt(8)])),
    Column("sessions_crashed_preaggr", AggregateFunction("sumIf", [UInt(32), UInt(8)])),
    Column("sessions_abnormal", AggregateFunction("countIf", [UUID(), UInt(8)])),
    Column(
        "sessions_abnormal_preaggr", AggregateFunction("sumIf", [UInt(32), UInt(8)])
    ),
    Column("sessions_errored", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
    Column("sessions_errored_preaggr", AggregateFunction("sumIf", [UInt(32), UInt(8)])),
    # users:
    Column("users", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
    Column("users_crashed", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
    Column("users_abnormal", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
    Column("users_errored", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
]


query = f"""
SELECT
    org_id,
    project_id,
    toStartOfHour(started) as started,
    release,
    environment,
    user_agent,
    os,

    -- pre-aggregated sessions dont have a duration, so no more filtering is needed.
    -- we would have liked to changed the quantiles here, but the data structure allows
    -- querying arbitrary quantiles from the `quantilesState`:
    quantilesIfState(0.5, 0.9)(
        duration,
        duration <> {MAX_UINT32} AND status == 1
    ) as duration_quantiles,

    -- this is new, and similarly as above pre-aggregated sessions dont have a duration:
    avgIfState(duration, duration <> {MAX_UINT32} AND status == 1) as duration_avg,

    -- `sum` the session counts based on the new `quantity`:
    sumIfState(quantity, seq == 0) as sessions_preaggr,
    sumIfState(quantity, status == 2) as sessions_crashed_preaggr,
    sumIfState(quantity, status == 3) as sessions_abnormal_preaggr,

    -- individual session updates keep using the uniq session_id as before:
    uniqIfState(session_id, errors > 0 AND session_id != '{NIL_UUID}') as sessions_errored,

    -- pre-aggregated counts use sum. by definition, `crashed` and `abnormal` are errored:
    sumIfState(quantity, status IN (2, 3, 4) AND session_id == '{NIL_UUID}') as sessions_errored_preaggr,

    -- users counts will additionally be constrained for the distinct-id:
    uniqIfState(distinct_id, distinct_id != '{NIL_UUID}') as users,
    uniqIfState(distinct_id, status == 2 AND distinct_id != '{NIL_UUID}') as users_crashed,
    uniqIfState(distinct_id, status == 3 AND distinct_id != '{NIL_UUID}') as users_abnormal,
    uniqIfState(distinct_id, errors > 0 AND distinct_id != '{NIL_UUID}') as users_errored

FROM
     sessions_raw_local
GROUP BY
     org_id, project_id, started, release, environment, user_agent, os
"""


class Migration(migration.MultiStepMigration):
    """
    This migration re-creates the materialized view that aggregates sessions.
    It is now using the new `X_preaggr` columns based on the `quantity`.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_mv_local",
            ),
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.SESSIONS,
                view_name="sessions_hourly_mv_local",
                destination_table_name="sessions_hourly_local",
                columns=aggregate_columns,
                query=query,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_mv_local",
            ),
            create_matview_v1,
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return []

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return []
