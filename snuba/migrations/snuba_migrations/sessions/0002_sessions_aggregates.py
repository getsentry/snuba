from typing import Sequence

from snuba.clickhouse.columns import (
    AggregateFunction,
    Column,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


DEFAULT_QUANTITY = 1


class Migration(migration.MultiStepMigration):
    """
    This is the first migration that adds new columns to the raw and materialized
    sessions dataset.
    A second migration will be added later that updates the materialization query
    to use the `quantity` raw column and feed that into the `X_sum` columns.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_raw_local",
                column=Column(
                    "quantity", UInt(32, Modifiers(default=str(DEFAULT_QUANTITY)))
                ),
                after="distinct_id",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                column=Column(
                    "duration_avg", AggregateFunction("avgIf", [UInt(32), UInt(8)])
                ),
                after="duration_quantiles",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                column=Column(
                    "sessions_sum", AggregateFunction("sumIf", [UInt(32), UInt(8)])
                ),
                after="sessions",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                column=Column(
                    "sessions_crashed_sum",
                    AggregateFunction("sumIf", [UInt(32), UInt(8)]),
                ),
                after="sessions_crashed",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                column=Column(
                    "sessions_abnormal_sum",
                    AggregateFunction("sumIf", [UInt(32), UInt(8)]),
                ),
                after="sessions_abnormal",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                column=Column(
                    "sessions_errored_sum",
                    AggregateFunction("sumIf", [UInt(32), UInt(8)]),
                ),
                after="sessions_errored",
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
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_raw_dist",
                column=Column(
                    "quantity", UInt(32, Modifiers(default=str(DEFAULT_QUANTITY)))
                ),
                after="distinct_id",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                column=Column(
                    "duration_avg", AggregateFunction("avgIf", [UInt(32), UInt(8)])
                ),
                after="duration_quantiles",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                column=Column(
                    "sessions_sum", AggregateFunction("sumIf", [UInt(32), UInt(8)])
                ),
                after="sessions",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                column=Column(
                    "sessions_crashed_sum",
                    AggregateFunction("sumIf", [UInt(32), UInt(8)]),
                ),
                after="sessions_crashed",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                column=Column(
                    "sessions_abnormal_sum",
                    AggregateFunction("sumIf", [UInt(32), UInt(8)]),
                ),
                after="sessions_abnormal",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                column=Column(
                    "sessions_errored_sum",
                    AggregateFunction("sumIf", [UInt(32), UInt(8)]),
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
