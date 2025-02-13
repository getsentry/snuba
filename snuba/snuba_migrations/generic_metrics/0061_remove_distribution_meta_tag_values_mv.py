from typing import Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import Float


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    granularity = "2048"
    tag_value_view_name = "generic_metric_distributions_meta_tag_values_mv"
    tag_value_view_name = "generic_metric_distributions_meta_tag_values_mv"
    tag_value_local_table_name = "generic_metric_distributions_meta_tag_values_local"
    tag_value_dist_table_name = "generic_metric_distributions_meta_tag_values_dist"
    tag_value_table_columns: Sequence[Column[Modifiers]] = [
        Column("project_id", UInt(64)),
        Column("metric_id", UInt(64)),
        Column("tag_key", UInt(64)),
        Column("tag_value", String()),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["DoubleDelta"]))),
        Column("retention_days", UInt(16)),
        Column("count", AggregateFunction("sum", [Float(64)])),
    ]
    storage_set_key = StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.tag_value_view_name,
                target=OperationTarget.LOCAL,
            )
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.tag_value_view_name,
                columns=self.tag_value_table_columns,
                destination_table_name=self.tag_value_local_table_name,
                target=OperationTarget.LOCAL,
                query="""
                CREATE MATERIALIZED VIEW snuba_test.generic_metric_distributions_meta_tag_values_mv
                TO snuba_test.generic_metric_distributions_meta_tag_values_local
                (`project_id` UInt64, `metric_id` UInt64, `tag_key` UInt64, `tag_value` String, `timestamp` DateTime CODEC(DoubleDelta), `retention_days` UInt16, `count` AggregateFunction(sum, Float64)) AS SELECT project_id, metric_id, tag_key, tag_value, toMonday(timestamp) AS timestamp, retention_days, sumState(count_value) AS count FROM snuba_test.generic_metric_distributions_raw_local ARRAY JOIN tags.key AS tag_key, tags.raw_value AS tag_value WHERE record_meta = 1 GROUP BY project_id, metric_id, tag_key, tag_value, timestamp, retention_days
                """,
            )
        ]
