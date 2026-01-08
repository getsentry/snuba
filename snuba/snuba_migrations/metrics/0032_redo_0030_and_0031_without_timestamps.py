from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.snuba_migrations.metrics.templates import (
    COL_SCHEMA_DISTRIBUTIONS_V2,
    get_forward_view_migration_polymorphic_table_v3,
    get_polymorphic_mv_variant_name,
)
from snuba.utils.schemas import AggregateFunction, Column, UInt


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Re-creates materialized views of 0030 and 0031 without timestamps for easier operations
    """

    blocking = False
    dist_table_name = "metrics_distributions_v2_local"
    sets_table_name = "metrics_sets_v2_local"
    raw_table_name = "metrics_raw_v2_local"
    mv_version = 4

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS,
                table_name=get_polymorphic_mv_variant_name("distributions", self.mv_version),
            ),
            get_forward_view_migration_polymorphic_table_v3(
                source_table_name=self.raw_table_name,
                table_name=self.dist_table_name,
                aggregation_col_schema=COL_SCHEMA_DISTRIBUTIONS_V2,
                mv_name=get_polymorphic_mv_variant_name("distributions", self.mv_version),
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
                target_mat_version=4,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.METRICS,
                table_name=get_polymorphic_mv_variant_name("sets", self.mv_version),
            ),
            get_forward_view_migration_polymorphic_table_v3(
                source_table_name=self.raw_table_name,
                table_name=self.sets_table_name,
                aggregation_col_schema=[
                    Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
                ],
                aggregation_states="uniqCombined64State(arrayJoin(set_values)) as value",
                mv_name=get_polymorphic_mv_variant_name("sets", self.mv_version),
                metric_type="set",
                target_mat_version=4,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
