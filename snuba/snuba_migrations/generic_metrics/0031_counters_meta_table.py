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
    local_table_name = "generic_metric_counters_meta_aggregated_local"
    dist_table_name = "generic_metric_counters_meta_aggregated_dist"
    storage_set_key = StorageSetKey.GENERIC_METRICS_COUNTERS
    columns: Sequence[Column[Modifiers]] = [
        Column("org_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("use_case_id", String(Modifiers(low_cardinality=True))),
        Column("metric_id", UInt(64)),
        Column("tag_key", String()),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["DoubleDelta"]))),
        Column("retention_days", UInt(16)),
        Column("tag_values", AggregateFunction("groupUniqArray", [String()])),
        Column("count", AggregateFunction("sum", [Float(64)])),
    ]

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    order_by="(org_id, project_id, use_case_id, metric_id, tag_key, timestamp)",
                    primary_key="(org_id, project_id, use_case_id, metric_id, tag_key, timestamp)",
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
