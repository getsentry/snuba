from importlib import import_module
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
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.processor import MAX_UINT32, NIL_UUID


v1_matview: operations.Operation = import_module(
    "snuba.migrations.snuba_migrations.sessions.0001_sessions"
).create_matview


aggregate_columns: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("started", DateTime()),
    Column("release", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(low_cardinality=True))),
    Column(
        "duration_quantiles",
        AggregateFunction("quantilesIf(0.5, 0.9)", [UInt(32), UInt(8)]),
    ),
    Column("duration_avg", AggregateFunction("avgIf", [UInt(32), UInt(8)])),
    # sessions:
    Column("sessions_sum", AggregateFunction("sumIf", [UInt(32), UInt(8)])),
    Column("sessions_crashed_sum", AggregateFunction("sumIf", [UInt(32), UInt(8)])),
    Column("sessions_abnormal_sum", AggregateFunction("sumIf", [UInt(32), UInt(8)])),
    Column("sessions_errored", AggregateFunction("uniqIf", [UUID(), UInt(8)])),
    Column("sessions_errored_sum", AggregateFunction("sumIf", [UInt(32), UInt(8)])),
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

  -- we would have liked to changed the quantiles here to `0.5, 0.75, 0.95, 0.99`:
  quantilesIfState(0.5, 0.9)(
    duration,
    duration <> {MAX_UINT32} AND status == 1
  ) as duration_quantiles,

  -- this is new:
  avgIfState(duration, duration <> {MAX_UINT32} AND status == 1) as duration_avg,

  -- instead of `count`, we use `sum` of `quantity`
  sumIfState(quantity, seq == 0) as sessions_sum,
  sumIfState(quantity, status == 2) as sessions_crashed_sum,
  sumIfState(quantity, status == 3) as sessions_abnormal_sum,

  -- combination of `sum` of `quantity`, and a `uniq` for "old" individual sessions

  -- "old" individual sessions use the uniq session_id as before:
  uniqIfState(session_id, errors > 0 AND session_id != '{NIL_UUID}') as sessions_errored,

  -- "new" aggregate counts use sum. by definition, `crashed` and `abnormal` are errored
  sumIfState(quantity, status IN (2, 3, 4) AND session_id == '{NIL_UUID}') as sessions_errored_sum,

  -- users counts will additionally be constrained for the distinct-id
  uniqIfState(distinct_id, distinct_id != '{NIL_UUID}') as users,
  uniqIfState(distinct_id, status == 2 AND distinct_id != '{NIL_UUID}') as users_crashed,
  uniqIfState(distinct_id, status == 3 AND distinct_id != '{NIL_UUID}') as users_abnormal,
  uniqIfState(distinct_id, errors > 0 AND distinct_id != '{NIL_UUID}') as users_errored

FROM
  sessions_raw_local
GROUP BY
  org_id, project_id, started, release, environment
"""


class Migration(migration.MultiStepMigration):
    """
    This is the second migration that hooks up the newly created aggregation columns.
    It comes after a first migration that added the needed columns, and after
    a feature PR that allows querying them.
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
            v1_matview,
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return []

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return []
