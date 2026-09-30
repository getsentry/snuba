from collections.abc import Sequence

from snuba.clickhouse.columns import Column, SimpleAggregateFunction, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

# `profiling_type` was added to the AggregatingMergeTree `functions_mv_local` table in
# 0004 as a plain column that is not part of the sorting key. During merges ClickHouse
# keeps an arbitrary value for such columns, and starting with ClickHouse 26.8 creating
# such a table is rejected unless `allow_dimensions_outside_sorting_key = 1` is set.
#
# Wrapping it in `SimpleAggregateFunction(any, ...)` makes the merge behaviour explicit
# and keeps the on-disk representation identical, so this is a metadata-only change.
new_column: Column[Modifiers] = Column(
    "profiling_type",
    SimpleAggregateFunction(
        "any",
        [String(Modifiers(low_cardinality=True))],
        Modifiers(default="'transaction'"),
    ),
)

old_column: Column[Modifiers] = Column(
    "profiling_type",
    String(Modifiers(low_cardinality=True, default="'transaction'")),
)


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set = StorageSetKey.FUNCTIONS

    local_materialized_table = "functions_mv_local"
    dist_materialized_table = "functions_mv_dist"

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=self.storage_set,
                table_name=table_name,
                column=new_column,
                target=target,
            )
            for table_name, target in [
                (self.local_materialized_table, OperationTarget.LOCAL),
                (self.dist_materialized_table, OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=self.storage_set,
                table_name=table_name,
                column=old_column,
                target=target,
            )
            for table_name, target in [
                (self.dist_materialized_table, OperationTarget.DISTRIBUTED),
                (self.local_materialized_table, OperationTarget.LOCAL),
            ]
        ]
