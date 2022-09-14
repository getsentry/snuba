from typing import Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, Float
from snuba.clusters.storage_set_key import StorageSetKey
from snuba.migrations import migration, operations
from snuba.snuba_migrations.metrics.templates import (
    get_consolidated_mv_name,
    get_forward_view_migration_local_consolidated,
)


class Migration(migration.ClickhouseNodeMigration):
    """
    Create a materialized view for metrics counters which writes to 10s, 1m, 1h, 1d granularities

    The backward migration does *not* delete any data from the destination tables.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return (
            get_forward_view_migration_local_consolidated(
                source_table_name="metrics_counters_buckets_local",
                table_name="metrics_counters_local",
                mv_name=get_consolidated_mv_name("counters"),
                aggregation_col_schema=[
                    Column("value", AggregateFunction("sum", [Float(64)])),
                ],
                aggregation_states="sumState(value) as value",
            ),
        )

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS,
                table_name=get_consolidated_mv_name("counters"),
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
