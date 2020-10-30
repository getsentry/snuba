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
from snuba.migrations import operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.processor import MAX_UINT32, NIL_UUID

aggregate_columns_v1: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("started", DateTime()),
    Column("release", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(low_cardinality=True))),
    Column(
        "duration_quantiles",
        AggregateFunction("quantilesIf(0.5, 0.9)", [UInt(32), UInt(8)]),
    ),
    Column("sessions", AggregateFunction("countIf", [UUID(), UInt(8)])),
    Column("users", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
    Column("sessions_crashed", AggregateFunction("countIf", [UUID(), UInt(8)])),
    Column("sessions_abnormal", AggregateFunction("countIf", [UUID(), UInt(8)])),
    Column("sessions_errored", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
    Column("users_crashed", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
    Column("users_abnormal", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
    Column("users_errored", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
]

query_v1 = f"""
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
"""

create_matview_v1 = operations.CreateMaterializedView(
    storage_set=StorageSetKey.SESSIONS,
    view_name="sessions_hourly_mv_local",
    destination_table_name="sessions_hourly_local",
    columns=aggregate_columns_v1,
    query=query_v1,
)
