from typing import Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.snuba_migrations.metrics.templates import (
    get_forward_view_migration_polymorphic_table_v3,
    get_polymorphic_mv_variant_name,
)


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Add a materialized view for writing to the v2 version of the metrics_sets aggregate table
    """

    blocking = False
    table_name = "metrics_sets_v2_local"
    raw_table_name = "metrics_raw_v2_local"
    mv_version = 4

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            get_forward_view_migration_polymorphic_table_v3(
                source_table_name=self.raw_table_name,
                table_name=self.table_name,
                aggregation_col_schema=[
                    Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
                ],
                aggregation_states="uniqCombined64State(arrayJoin(set_values)) as value",
                mv_name=get_polymorphic_mv_variant_name("sets", self.mv_version),
                metric_type="set",
                target_mat_version=4,
                appended_where_clause="AND timestamp > toDateTime('2022-03-29 00:00:00')",
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS,
                table_name=get_polymorphic_mv_variant_name("sets", self.mv_version),
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
