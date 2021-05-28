from typing import Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, Float
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.snuba_migrations.metrics.templates import (
    get_forward_migrations_dist,
    get_forward_migrations_local,
    get_reverse_table_migration,
)

COL_SCHEMA: Sequence[Column[MigrationModifiers]] = [
    Column("percentiles", AggregateFunction("quantiles(0.5, 0.9, 0.99)", [Float(64)]),),
    Column("min", AggregateFunction("min", [Float(64)])),
    Column("max", AggregateFunction("max", [Float(64)])),
    Column("avg", AggregateFunction("avg", [Float(64)])),
    Column("sum", AggregateFunction("sum", [Float(64)])),
    Column("count", AggregateFunction("count", [Float(64)])),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return get_forward_migrations_local(
            source_table_name="metrics_distributions_buckets_local",
            table_name="metrics_distributions_local",
            mv_name="metrics_distributions_mv_local",
            aggregation_col_schema=COL_SCHEMA,
            aggregation_states=(
                "quantilesState(0.5, 0.75, 0.9, 0.95, 0.99, 1)((arrayJoin(values) AS values_rows)) as percentiles, "
                "minState(values_rows) as min, "
                "maxState(values_rows) as max, "
                "avgState(values_rows) as avg, "
                "sumState(values_rows) as sum, "
                "countState(values_rows) as count"
            ),
        )

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            *get_reverse_table_migration("metrics_distributions_mv_local"),
            *get_reverse_table_migration("metrics_distributions_local"),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return get_forward_migrations_dist(
            dist_table_name="metrics_distributions_dist",
            local_table_name="metrics_distributions_local",
            aggregation_col_schema=COL_SCHEMA,
        )

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return get_reverse_table_migration("metrics_distributions_dist")
