from typing import Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import Float


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    granularity = "2048"
    local_table_name = "generic_metric_counters_meta_tag_value_aggregated_local"
    dist_table_name = "generic_metric_counters_meta_tag_value_aggregated_dist"
    storage_set_key = StorageSetKey.GENERIC_METRICS_COUNTERS
    columns: Sequence[Column[Modifiers]] = [
        Column("project_id", UInt(64)),
        Column("metric_id", UInt(64)),
        Column("tag_key", UInt(64)),
        Column("tag_value", String()),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["DoubleDelta"]))),
        Column("retention_days", UInt(16)),
        Column("count", AggregateFunction("sum", [Float(64)])),
    ]

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    order_by="(project_id, metric_id, tag_key, tag_value, timestamp)",
                    primary_key="(project_id, metric_id, tag_key, tag_value, timestamp)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={"index_granularity": self.granularity},
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                columns=self.columns,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.local_table_name, sharding_key=None
                ),
                columns=self.columns,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                target=OperationTarget.LOCAL,
            ),
        ]
