from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.snuba_migrations.metrics.templates import (
    COL_SCHEMA_DISTRIBUTIONS,
    get_forward_view_migration_polymorphic_table,
    get_polymorphic_mv_v2_name,
)
from snuba.utils.schemas import AggregateFunction, Column, Float, UInt


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Creates materialized view for all metrics types which writes to 10s, 1m, 1h, 1d granularities
    The backward migration does *not* delete any data from the destination tables.
    """

    blocking = False
    raw_table_name = "metrics_raw_v2_local"

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            get_forward_view_migration_polymorphic_table(
                source_table_name=self.raw_table_name,
                table_name="metrics_distributions_local",
                mv_name=get_polymorphic_mv_v2_name("distributions"),
                aggregation_col_schema=COL_SCHEMA_DISTRIBUTIONS,
                aggregation_states=(
                    "quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)((arrayJoin(distribution_values) AS values_rows)) as percentiles, "
                    "minState(values_rows) as min, "
                    "maxState(values_rows) as max, "
                    "avgState(values_rows) as avg, "
                    "sumState(values_rows) as sum, "
                    "countState(values_rows) as count"
                ),
                metric_type="distribution",
            ),
            get_forward_view_migration_polymorphic_table(
                source_table_name=self.raw_table_name,
                table_name="metrics_sets_local",
                mv_name=get_polymorphic_mv_v2_name("sets"),
                aggregation_col_schema=[
                    Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
                ],
                aggregation_states="uniqCombined64State(arrayJoin(set_values)) as value",
                metric_type="set",
            ),
            get_forward_view_migration_polymorphic_table(
                source_table_name=self.raw_table_name,
                table_name="metrics_counters_local",
                mv_name=get_polymorphic_mv_v2_name("counters"),
                aggregation_col_schema=[
                    Column("value", AggregateFunction("sum", [Float(64)])),
                ],
                aggregation_states="sumState(count_value) as value",
                metric_type="counter",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS,
                table_name=get_polymorphic_mv_v2_name(mv_type),
            )
            for mv_type in ["sets", "counters", "distributions"]
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
