from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.snuba_migrations.metrics.templates import (
    COL_SCHEMA_DISTRIBUTIONS_V2,
    get_forward_view_migration_polymorphic_table_v3,
    get_polymorphic_mv_variant_name,
)


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Add a materialized view for writing to the v2 version of the metrics_distributions aggregate table
    """

    blocking = False
    table_name = "metrics_distributions_v2_local"
    raw_table_name = "metrics_raw_v2_local"
    mv_version = 4

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            get_forward_view_migration_polymorphic_table_v3(
                source_table_name=self.raw_table_name,
                table_name=self.table_name,
                aggregation_col_schema=COL_SCHEMA_DISTRIBUTIONS_V2,
                mv_name=get_polymorphic_mv_variant_name(
                    "distributions", self.mv_version
                ),
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
                appended_where_clause="AND timestamp >= toDateTime('2022-03-29 00:00:00')",
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS,
                table_name=get_polymorphic_mv_variant_name(
                    "distributions", self.mv_version
                ),
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
