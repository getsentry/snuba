from typing import Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, Float, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.snuba_migrations.metrics.templates import (
    COL_SCHEMA_DISTRIBUTIONS_V2,
    get_forward_view_migration_polymorphic_table_v2,
    get_polymorphic_mv_v3_name,
)


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Add HdrHistogram to distribution metrics.
    """

    blocking = False
    raw_table_name = "metrics_raw_v2_local"

    def __forward_migrations(self, table_name: str) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.METRICS,
                table_name=table_name,
                column=Column(
                    "histogram_buckets",
                    AggregateFunction("histogram(250)", [Float(64)]),
                ),
                after="count",
            )
        ]

    def __backward_migrations(self, table_name: str) -> Sequence[operations.SqlOperation]:
        return [
            # operations.DropColumn(
            #     storage_set=StorageSetKey.METRICS,
            #     table_name=table_name,
            #     column_name="histogram_buckets",
            # )
        ]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            *self.__forward_migrations("metrics_distributions_local"),
            get_forward_view_migration_polymorphic_table_v2(
                source_table_name=self.raw_table_name,
                table_name="metrics_distributions_local",
                mv_name=get_polymorphic_mv_v3_name("distributions"),
                aggregation_col_schema=COL_SCHEMA_DISTRIBUTIONS_V2,
                aggregation_states=(
                    "quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)((arrayJoin(distribution_values) AS values_rows)) as percentiles, "
                    "minState(values_rows) as min, "
                    "maxState(values_rows) as max, "
                    "avgState(values_rows) as avg, "
                    "sumState(values_rows) as sum, "
                    "countState(values_rows) as count, "
                    "histogramState(250)(values_rows) as histogram_buckets"
                ),
                metric_type="distribution",
                materialization_version=4,
            ),
            # No changes in those MV's schema. We just need to recreate the
            # same exact MV as in 0023 for the new materialization_version
            get_forward_view_migration_polymorphic_table_v2(
                source_table_name=self.raw_table_name,
                table_name="metrics_sets_local",
                mv_name=get_polymorphic_mv_v3_name("sets"),
                aggregation_col_schema=[
                    Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
                ],
                aggregation_states="uniqCombined64State(arrayJoin(set_values)) as value",
                metric_type="set",
                materialization_version=4,
            ),
            get_forward_view_migration_polymorphic_table_v2(
                source_table_name=self.raw_table_name,
                table_name="metrics_counters_local",
                mv_name=get_polymorphic_mv_v3_name("counters"),
                aggregation_col_schema=[
                    Column("value", AggregateFunction("sum", [Float(64)])),
                ],
                aggregation_states="sumState(count_value) as value",
                metric_type="counter",
                materialization_version=4,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            *self.__backward_migrations("metrics_distributions_local"),
            *[
                operations.DropTable(
                    storage_set=StorageSetKey.METRICS,
                    table_name=get_polymorphic_mv_v3_name(mv_type),
                )
                for mv_type in ["sets", "counters", "distributions"]
            ],
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("metrics_distributions_dist")

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__backward_migrations("metrics_distributions_dist")
