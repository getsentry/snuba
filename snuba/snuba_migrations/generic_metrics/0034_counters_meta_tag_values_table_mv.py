from typing import Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import Float


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    view_name = "generic_metric_counters_meta_aggregation_mv"
    dest_table_name = "generic_metric_counters_meta_tag_value_aggregated_local"
    dest_table_columns: Sequence[Column[Modifiers]] = [
        Column("project_id", UInt(64)),
        Column("metric_id", UInt(64)),
        Column("tag_key", UInt(64)),
        Column("tag_value", String()),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["DoubleDelta"]))),
        Column("retention_days", UInt(16)),
        Column("count", AggregateFunction("sum", [Float(64)])),
    ]
    storage_set_key = StorageSetKey.GENERIC_METRICS_COUNTERS

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.view_name,
                columns=self.dest_table_columns,
                destination_table_name=self.dest_table_name,
                target=OperationTarget.LOCAL,
                query="""
                SELECT
                    project_id,
                    metric_id,
                    tag_key,
                    tag_value,
                    toStartOfWeek(timestamp) as timestamp,
                    retention_days,
                    sumState(count_value) as count
                FROM generic_metric_counters_raw_local
                ARRAY JOIN
                    tags.key AS tag_key, tags.raw_value AS tag_value
                WHERE record_meta = 1
                GROUP BY
                    project_id,
                    metric_id,
                    tag_key,
                    tag_value,
                    timestamp,
                    retention_days
                """,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.view_name,
                target=OperationTarget.LOCAL,
            )
        ]
