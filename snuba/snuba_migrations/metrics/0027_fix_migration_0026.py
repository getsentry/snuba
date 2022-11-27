from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.snuba_migrations.metrics.templates import (
    get_forward_view_migration_polymorphic_table_v3,
    get_polymorphic_mv_variant_name,
)
from snuba.utils.schemas import AggregateFunction, Column, Float


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Migration 0026 reused a materialized view name and then failed silently because
    of a CREATE ... IF NOT EXISTS clause
    """

    blocking = False
    table_name = "metrics_counters_v2_local"
    raw_table_name = "metrics_raw_v2_local"
    mv_version = 4

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            get_forward_view_migration_polymorphic_table_v3(
                source_table_name=self.raw_table_name,
                table_name=self.table_name,
                aggregation_col_schema=[
                    Column("value", AggregateFunction("sum", [Float(64)])),
                ],
                mv_name=get_polymorphic_mv_variant_name("counters", self.mv_version),
                aggregation_states="sumState(count_value) as value",
                metric_type="counter",
                target_mat_version=4,
                appended_where_clause="AND 1=1",
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS,
                table_name=get_polymorphic_mv_variant_name("counters", self.mv_version),
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
