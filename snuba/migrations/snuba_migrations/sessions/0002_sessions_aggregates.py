from typing import Sequence, Tuple

from snuba.clickhouse.columns import (
    AggregateFunction,
    Column,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

DEFAULT_QUANTITY = 1

new_raw_columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (
        Column("quantity", UInt(32, Modifiers(default=str(DEFAULT_QUANTITY)))),
        "distinct_id",
    ),
    (Column("user_agent", String(Modifiers(low_cardinality=True))), "environment"),
    (Column("os", String(Modifiers(low_cardinality=True))), "user_agent"),
]

new_dest_columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (Column("user_agent", String(Modifiers(low_cardinality=True))), "environment"),
    (Column("os", String(Modifiers(low_cardinality=True))), "user_agent"),
    (
        Column("duration_avg", AggregateFunction("avgIf", [UInt(32), UInt(8)])),
        "duration_quantiles",
    ),
    (
        Column("sessions_preaggr", AggregateFunction("sumIf", [UInt(32), UInt(8)])),
        "sessions",
    ),
    (
        Column(
            "sessions_crashed_preaggr", AggregateFunction("sumIf", [UInt(32), UInt(8)]),
        ),
        "sessions_crashed",
    ),
    (
        Column(
            "sessions_abnormal_preaggr",
            AggregateFunction("sumIf", [UInt(32), UInt(8)]),
        ),
        "sessions_abnormal",
    ),
    (
        Column(
            "sessions_errored_preaggr", AggregateFunction("sumIf", [UInt(32), UInt(8)]),
        ),
        "sessions_errored",
    ),
]


class Migration(migration.MultiStepMigration):
    """
    This migration adds new columns to both the raw and aggregated sessions.
    The new `X_preaggr` columns in the aggregated dataset will be used later on
    when the consumer provides a `quantity`.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_raw_local",
                column=column,
                after=after,
            )
            for [column, after] in new_raw_columns
        ] + [
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                column=column,
                after=after,
            )
            for [column, after] in new_dest_columns
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_raw_local", column.name
            )
            for [column, after] in new_raw_columns
        ] + [
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_hourly_local", column.name
            )
            for [column, after] in new_dest_columns
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_raw_dist",
                column=column,
                after=after,
            )
            for [column, after] in new_raw_columns
        ] + [
            operations.AddColumn(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                column=column,
                after=after,
            )
            for [column, after] in new_dest_columns
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_raw_dist", column.name
            )
            for [column, after] in new_raw_columns
        ] + [
            operations.DropColumn(
                StorageSetKey.SESSIONS, "sessions_hourly_dist", column.name
            )
            for [column, after] in new_dest_columns
        ]
