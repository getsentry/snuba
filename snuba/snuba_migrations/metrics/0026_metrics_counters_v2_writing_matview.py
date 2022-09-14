from typing import Sequence

from snuba.clusters.storage_set_key import StorageSetKey
from snuba.migrations import migration, operations
from snuba.snuba_migrations.metrics.templates import (
    get_forward_view_migration_polymorphic_table_v3,
    get_polymorphic_mv_v3_name,
)
from snuba.utils.schemas import AggregateFunction, Column, Float


class Migration(migration.ClickhouseNodeMigration):
    """
    Creates materialized view for all metrics types which writes to 10s, 1m, 1h, 1d granularities
    The backward migration does *not* delete any data from the destination tables.
    """

    blocking = False
    table_name = "metrics_counters_v2_local"
    raw_table_name = "metrics_raw_v2_local"

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            get_forward_view_migration_polymorphic_table_v3(
                source_table_name=self.raw_table_name,
                table_name=self.table_name,
                aggregation_col_schema=[
                    Column("value", AggregateFunction("sum", [Float(64)])),
                ],
                mv_name=get_polymorphic_mv_v3_name("counters"),
                aggregation_states="sumState(count_value) as value",
                metric_type="counter",
                target_mat_version=4,
                appended_where_clause="AND timestamp > toDateTime('2022-03-24 16:00:00')",
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS,
                table_name=get_polymorphic_mv_v3_name("counters"),
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
