from typing import Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import Float


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    granularity = "8192"
    meta_view_name = "generic_metric_distributions_meta_mv"
    meta_local_table_name = "generic_metric_distributions_meta_local"
    meta_dist_table_name = "generic_metric_distributions_meta_dist"
    meta_table_columns: Sequence[Column[Modifiers]] = [
        Column("org_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("use_case_id", String(Modifiers(low_cardinality=True))),
        Column("metric_id", UInt(64)),
        Column("tag_key", UInt(64)),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["DoubleDelta"]))),
        Column("retention_days", UInt(16)),
        Column("count", AggregateFunction("sum", [Float(64)])),
    ]

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
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_local_table_name,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    order_by="(org_id, project_id, use_case_id, metric_id, tag_key, timestamp)",
                    primary_key="(org_id, project_id, use_case_id, metric_id, tag_key, timestamp)",
                    partition_by="toMonday(timestamp)",
                    settings={
                        "index_granularity": self.granularity,
                        # Since the partitions contain multiple retention periods, need to ensure
                        # that rows within partitions are dropped
                        "ttl_only_drop_parts": 0,
                    },
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                columns=self.meta_table_columns,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.meta_local_table_name, sharding_key=None
                ),
                columns=self.meta_table_columns,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.meta_view_name,
                columns=self.meta_table_columns,
                destination_table_name=self.meta_local_table_name,
                target=OperationTarget.LOCAL,
                query="""
                SELECT
                    org_id,
                    project_id,
                    use_case_id,
                    metric_id,
                    tag_key,
                    toMonday(timestamp) as timestamp,
                    retention_days,
                    sumState(count_value) as count
                FROM generic_metric_distributions_raw_local
                ARRAY JOIN tags.key AS tag_key
                WHERE record_meta = 1
                GROUP BY
                    org_id,
                    project_id,
                    use_case_id,
                    metric_id,
                    tag_key,
                    timestamp,
                    retention_days
                """,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.tag_value_local_table_name,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    order_by="(project_id, metric_id, tag_key, tag_value, timestamp)",
                    primary_key="(project_id, metric_id, tag_key, tag_value, timestamp)",
                    partition_by="toMonday(timestamp)",
                    settings={
                        "index_granularity": self.granularity,
                        # Since the partitions contain multiple retention periods, need to ensure
                        # that rows within partitions are dropped
                        "ttl_only_drop_parts": 0,
                    },
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                columns=self.tag_value_table_columns,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.tag_value_dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.tag_value_local_table_name, sharding_key=None
                ),
                columns=self.tag_value_table_columns,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.tag_value_view_name,
                columns=self.tag_value_table_columns,
                destination_table_name=self.tag_value_local_table_name,
                target=OperationTarget.LOCAL,
                query="""
                SELECT
                    project_id,
                    metric_id,
                    tag_key,
                    tag_value,
                    toMonday(timestamp) as timestamp,
                    retention_days,
                    sumState(count_value) as count
                FROM generic_metric_distributions_raw_local
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
                table_name=self.tag_value_view_name,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.tag_value_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.tag_value_local_table_name,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_view_name,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_local_table_name,
                target=OperationTarget.LOCAL,
            ),
        ]
