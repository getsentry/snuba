from typing import List, Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, Float, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.snuba_migrations.metrics.templates import (
    get_forward_view_migration_local,
)


def drop_views() -> List[operations.SqlOperation]:
    return [
        operations.DropTable(
            storage_set=StorageSetKey.METRICS,
            table_name=f"metrics_{metric_type}_mv_local",
        )
        for metric_type in ("sets", "counters", "distributions")
    ]


def recreate_views(granularity: int) -> List[operations.SqlOperation]:
    return [
        get_forward_view_migration_local(
            source_table_name="metrics_buckets_local",
            table_name="metrics_sets_local",
            mv_name="metrics_sets_mv_local",
            aggregation_col_schema=[
                Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
            ],
            aggregation_states="uniqCombined64State(arrayJoin(set_values)) as value",
            granularity=granularity,
        ),
        get_forward_view_migration_local(
            source_table_name="metrics_counters_buckets_local",
            table_name="metrics_counters_local",
            mv_name="metrics_counters_mv_local",
            aggregation_col_schema=[
                Column("value", AggregateFunction("sum", [Float(64)])),
            ],
            aggregation_states="sumState(value) as value",
            granularity=granularity,
        ),
        get_forward_view_migration_local(
            source_table_name="metrics_distributions_buckets_local",
            table_name="metrics_distributions_local",
            mv_name="metrics_distributions_mv_local",
            aggregation_col_schema=[
                Column(
                    "percentiles",
                    AggregateFunction(
                        "quantiles(0.5, 0.75, 0.9, 0.95, 0.99)", [Float(64)]
                    ),
                ),
                Column("min", AggregateFunction("min", [Float(64)])),
                Column("max", AggregateFunction("max", [Float(64)])),
                Column("avg", AggregateFunction("avg", [Float(64)])),
                Column("sum", AggregateFunction("sum", [Float(64)])),
                Column("count", AggregateFunction("count", [Float(64)])),
            ],
            aggregation_states=(
                "quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)((arrayJoin(values) AS values_rows)) as percentiles, "
                "minState(values_rows) as min, "
                "maxState(values_rows) as max, "
                "avgState(values_rows) as avg, "
                "sumState(values_rows) as sum, "
                "countState(values_rows) as count"
            ),
            granularity=granularity,
        ),
    ]


class Migration(migration.ClickhouseNodeMigration):
    """
    Re-create materialized views for metrics with a 10 second granularity
    instead of 60 seconds.

    The forward migration keeps the data rounded to start of minute, as there
    is no way of recovering granularity for existing entries.

    The backward migration could theoretically round existing entries to start of minute,
    but that is omitted for the sake of simplicity.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return drop_views() + recreate_views(granularity=10)

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return drop_views() + recreate_views(granularity=60)

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
