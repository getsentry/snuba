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
from snuba.migrations.columns import LowCardinality, WithDefault
from snuba.processor import MAX_UINT32, NIL_UUID


v1_matview = import_module(
    "snuba.migrations.snuba_migrations.sessions.0001_sessions"
).create_matview


DEFAULT_QUANTITY = 1


aggregate_columns = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("started", DateTime()),
    Column("release", LowCardinality(String())),
    Column("environment", LowCardinality(String())),
    Column(
        "duration_quantiles",
        AggregateFunction("quantilesIf(0.5, 0.75, 0.9, 0.95, 0.99)", UInt(32), UInt(8)),
    ),
    Column("duration_avg", AggregateFunction("avgIf", UInt(32), UInt(8))),
    # sessions:
    Column("sessions", AggregateFunction("sumIf", UInt(32), UInt(8))),
    Column("sessions_crashed", AggregateFunction("sumIf", UInt(32), UInt(8))),
    Column("sessions_abnormal", AggregateFunction("sumIf", UInt(32), UInt(8))),
    Column("sessions_errored_sum", AggregateFunction("sumIf", UInt(32), UInt(8))),
    Column("sessions_errored_uniq", AggregateFunction("uniqIf", UUID(), UInt(8))),
    # users:
    Column("users", AggregateFunction("uniqIf", UUID(), UInt(8))),
    Column("users_crashed", AggregateFunction("uniqIf", UUID(), UInt(8))),
    Column("users_abnormal", AggregateFunction("uniqIf", UUID(), UInt(8))),
    Column("users_errored", AggregateFunction("uniqIf", UUID(), UInt(8))),
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

  -- "new" aggregate counts use sum. by definition, `crashed` and `abnormal` are errored
  sumIfState(quantity, status IN (2, 3, 4) AND session_id == '{NIL_UUID}') as sessions_errored_sum,

  -- "old" individual sessions use the uniq session_id as before:
  uniqIfState(session_id, errors > 0 AND session_id != '{NIL_UUID}') as sessions_errored,

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
    Adds the `quantity` column to the raw data set, `duration_avg` and a few `X_sum` columns
    that will be used instead of the normal columns, which will be kept since we can’t
    change the column types from `countIf` to `sumIf`.
    The view query is also updated by re-creating it.

    *Ideally*, we would have just modified the `sessions_X` columns to be `sumIf`, but
    that is not possible, for that reason we keep the old columns around and just
    sum them at query time.
    Same for the `quantiles`, which we would have liked to change, but a `quantilesState`
    allows to query arbitrary quantiles independent of the definition.

    Also, it would be nice to rename all the `countIf` `sessions_X` columns to
    `sessions_X_count`, but that would require clickhouse 20.4.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            # add new columns
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_raw_local",
                column=Column("quantity", WithDefault(UInt(32), str(DEFAULT_QUANTITY))),
                after="distinct_id",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                column=Column(
                    "duration_avg", AggregateFunction("avgIf", UInt(32), UInt(8))
                ),
                after="duration_quantiles",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                column=Column(
                    "sessions_sum", AggregateFunction("sumIf", UInt(32), UInt(8))
                ),
                after="sessions",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                column=Column(
                    "sessions_crashed_sum",
                    AggregateFunction("sumIf", UInt(32), UInt(8)),
                ),
                after="sessions_crashed",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                column=Column(
                    "sessions_abnormal_sum",
                    AggregateFunction("sumIf", UInt(32), UInt(8)),
                ),
                after="sessions_abnormal",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                column=Column(
                    "sessions_errored_sum",
                    AggregateFunction("sumIf", UInt(32), UInt(8)),
                ),
                after="sessions_errored",
            ),
            # re-create the materialized view
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
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_raw_local", "quantity"
            ),
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_hourly_local", "duration_avg"
            ),
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_hourly_local", "sessions_sum"
            ),
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_hourly_local", "sessions_crashed_sum"
            ),
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_hourly_local", "sessions_abnormal_sum"
            ),
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_hourly_local", "sessions_errored_sum"
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_mv_local",
            ),
            v1_matview,
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_raw_dist",
                column=Column("quantity", WithDefault(UInt(32), str(DEFAULT_QUANTITY))),
                after="distinct_id",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                column=Column(
                    "duration_avg", AggregateFunction("avgIf", UInt(32), UInt(8))
                ),
                after="duration_quantiles",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                column=Column(
                    "sessions_sum", AggregateFunction("sumIf", UInt(32), UInt(8))
                ),
                after="sessions",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                column=Column(
                    "sessions_crashed_sum",
                    AggregateFunction("sumIf", UInt(32), UInt(8)),
                ),
                after="sessions_crashed",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                column=Column(
                    "sessions_abnormal_sum",
                    AggregateFunction("sumIf", UInt(32), UInt(8)),
                ),
                after="sessions_abnormal",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                column=Column(
                    "sessions_errored_sum",
                    AggregateFunction("sumIf", UInt(32), UInt(8)),
                ),
                after="sessions_errored",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_raw_dist", "quantity"
            ),
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_hourly_dist", "duration_avg"
            ),
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_hourly_dist", "sessions_sum"
            ),
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_hourly_dist", "sessions_crashed_sum"
            ),
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_hourly_dist", "sessions_abnormal_sum"
            ),
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_hourly_dist", "sessions_errored_sum"
            ),
        ]
