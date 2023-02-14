from typing import Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, Float
from snuba.migrations import migration, operations
from snuba.snuba_migrations.metrics.templates import (
    get_forward_migrations_dist,
    get_forward_migrations_local,
    get_migration_args_for_counters,
    get_reverse_table_migration,
)


class Migration(migration.ClickhouseNodeMigrationLegacy):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return get_forward_migrations_local(**get_migration_args_for_counters())

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            *get_reverse_table_migration("metrics_counters_mv_local"),
            *get_reverse_table_migration("metrics_counters_local"),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return get_forward_migrations_dist(
            dist_table_name="metrics_counters_dist",
            local_table_name="metrics_counters_local",
            aggregation_col_schema=[
                Column("value", AggregateFunction("sum", [Float(64)])),
            ],
        )

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return get_reverse_table_migration("metrics_counters_dist")
