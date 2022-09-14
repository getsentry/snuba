from typing import Sequence

from snuba.clusters.storage_set_key import StorageSetKey
from snuba.migrations import migration, operations
from snuba.snuba_migrations.metrics.templates import (
    COL_SCHEMA_DISTRIBUTIONS,
    get_consolidated_mv_name,
    get_forward_view_migration_local_consolidated,
)


class Migration(migration.ClickhouseNodeMigration):
    """
    Create a materialized view for metrics distributions which writes to 10s, 1m, 1h, 1d granularities

    The backward migration does *not* delete any data from the destination tables.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return (
            get_forward_view_migration_local_consolidated(
                source_table_name="metrics_distributions_buckets_local",
                table_name="metrics_distributions_local",
                mv_name=get_consolidated_mv_name("distributions"),
                aggregation_col_schema=COL_SCHEMA_DISTRIBUTIONS,
                aggregation_states=(
                    "quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)((arrayJoin(values) AS values_rows)) as percentiles, "
                    "minState(values_rows) as min, "
                    "maxState(values_rows) as max, "
                    "avgState(values_rows) as avg, "
                    "sumState(values_rows) as sum, "
                    "countState(values_rows) as count"
                ),
            ),
        )

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS,
                table_name=get_consolidated_mv_name("distributions"),
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
