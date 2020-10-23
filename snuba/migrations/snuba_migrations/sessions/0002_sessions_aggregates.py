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
from snuba.migrations.columns import LowCardinality, WithDefault
from snuba.processor import MAX_UINT32, NIL_UUID


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

  -- we changed the quantiles here:
  quantilesIfState(0.5, 0.75, 0.9, 0.95, 0.99)(
    duration,
    duration <> {MAX_UINT32} AND status == 1
  ) as duration_quantiles,

  -- this is new:
  avgIfState(duration, duration <> {MAX_UINT32} AND status == 1) as duration_avg,

  -- instead of `count`, we use `sum` of `quantity`
  sumIfState(quantity, seq == 0) as sessions,
  sumIfState(quantity, status == 2) as sessions_crashed,
  sumIfState(quantity, status == 3) as sessions_abnormal,

  -- combination of `sum` of `quantity`, and a `uniq` for "old" individual sessions

  -- "new" aggregate counts use sum. by definition, `crashed` and `abnormal` are errored
  sumIfState(quantity, status IN (2, 3, 4) AND session_id == '{NIL_UUID}') as sessions_errored_sum,

  -- "old" individual sessions use the uniq session_id as before:
  uniqIfState(session_id, errors > 0 AND session_id != '{NIL_UUID}') as sessions_errored_uniq,

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
    Adds the `quantity` column to the raw data set and re-creates the materialized view.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_raw_local",
                column=Column("quantity", WithDefault(UInt(32), str(DEFAULT_QUANTITY))),
                after="distinct_id",
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local_new",
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
                view_name="sessions_hourly_mv_local_new",
                destination_table_name="sessions_hourly_local_new",
                columns=aggregate_columns,
                query=query,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS, table_name="sessions_hourly_local",
            ),
            operations.RenameTable(
                storage_set=StorageSetKey.SESSIONS,
                old_table_name="sessions_hourly_local_new",
                new_table_name="sessions_hourly_local",
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_mv_local",
            ),
            operations.RenameTable(
                storage_set=StorageSetKey.SESSIONS,
                old_table_name="sessions_hourly_mv_local_new",
                new_table_name="sessions_hourly_mv_local",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_raw_local", "quantity"
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local_new",
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_mv_local_new",
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_raw_dist",
                column=Column("quantity", WithDefault(UInt(32), str(DEFAULT_QUANTITY))),
                after="distinct_id",
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist_new",
                columns=aggregate_columns,
                engine=table_engines.Distributed(
                    local_table_name="sessions_hourly_local", sharding_key="org_id",
                ),
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS, table_name="sessions_hourly_dist",
            ),
            operations.RenameTable(
                storage_set=StorageSetKey.SESSIONS,
                old_table_name="sessions_hourly_dist_new",
                new_table_name="sessions_hourly_dist",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_raw_dist", "quantity"
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist_new",
            ),
        ]
