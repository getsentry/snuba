from typing import Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, UInt
from snuba.migrations import migration, operations
from snuba.migrations.snuba_migrations.metrics.templates import (
    get_forward_migrations_dist,
    get_forward_migrations_local,
    get_reverse_mv_migration,
    get_reverse_table_migration,
)


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return get_forward_migrations_local(
            metric_type="sets",
            table_name="metrics_sets_local",
            mv_name="metrics_sets_mv_local",
            aggregation_col_schema=[
                Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
            ],
            aggregation_states="uniqCombined64State(arrayJoin(set_values)) as value",
        )

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            *get_reverse_mv_migration("metrics_sets_mv_local"),
            *get_reverse_table_migration("metrics_sets_local"),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return get_forward_migrations_dist(
            dist_table_name="metrics_sets_dist",
            local_table_name="metrics_sets_local",
            aggregation_col_schema=[
                Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
            ],
        )

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return get_reverse_table_migration("metrics_sets_dist")
