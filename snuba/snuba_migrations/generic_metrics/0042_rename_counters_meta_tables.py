from typing import Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import Float


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    granularity = "8192"
    new_meta_view_name = "generic_metric_counters_meta_mv"
    new_meta_local_table_name = "generic_metric_counters_meta_local"
    new_meta_dist_table_name = "generic_metric_counters_meta_dist"
    old_meta_view_name = "generic_metric_counters_meta_v2_mv"
    old_meta_local_table_name = "generic_metric_counters_meta_v2_local"
    old_meta_dist_table_name = "generic_metric_counters_meta_v2_dist"
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

    new_tag_value_view_name = "generic_metric_counters_meta_tag_values_mv"
    new_tag_value_local_table_name = "generic_metric_counters_meta_tag_values_local"
    new_tag_value_dist_table_name = "generic_metric_counters_meta_tag_values_dist"
    old_tag_value_view_name = "generic_metric_counters_meta_tag_values_v2_mv"
    old_tag_value_local_table_name = "generic_metric_counters_meta_tag_values_v2_local"
    old_tag_value_dist_table_name = "generic_metric_counters_meta_tag_values_v2_dist"
    tag_value_table_columns: Sequence[Column[Modifiers]] = [
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
        """
        The tables can't simply be renamed, since the materialized view depends on the specific table names.
        Drop the view, change the table names, then recreate the view with the correct table names.
        """

        meta_ops = [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.old_meta_view_name,
                target=OperationTarget.LOCAL,
            ),
            operations.RenameTable(
                storage_set=self.storage_set_key,
                old_table_name=self.old_meta_dist_table_name,
                new_table_name=self.new_meta_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.RenameTable(
                storage_set=self.storage_set_key,
                old_table_name=self.old_meta_local_table_name,
                new_table_name=self.new_meta_local_table_name,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.new_meta_view_name,
                columns=self.meta_table_columns,
                destination_table_name=self.new_meta_local_table_name,
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
                FROM generic_metric_counters_raw_local
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
        ]

        tag_value_ops = [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.old_tag_value_view_name,
                target=OperationTarget.LOCAL,
            ),
            operations.RenameTable(
                storage_set=self.storage_set_key,
                old_table_name=self.old_tag_value_dist_table_name,
                new_table_name=self.new_tag_value_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.RenameTable(
                storage_set=self.storage_set_key,
                old_table_name=self.old_tag_value_local_table_name,
                new_table_name=self.new_tag_value_local_table_name,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.new_tag_value_view_name,
                columns=self.tag_value_table_columns,
                destination_table_name=self.new_tag_value_local_table_name,
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

        return meta_ops + tag_value_ops

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        meta_ops = [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.new_meta_view_name,
                target=OperationTarget.LOCAL,
            ),
            operations.RenameTable(
                storage_set=self.storage_set_key,
                old_table_name=self.new_meta_dist_table_name,
                new_table_name=self.old_meta_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.RenameTable(
                storage_set=self.storage_set_key,
                old_table_name=self.new_meta_local_table_name,
                new_table_name=self.old_meta_local_table_name,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.old_meta_view_name,
                columns=self.meta_table_columns,
                destination_table_name=self.old_meta_local_table_name,
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
                FROM generic_metric_counters_raw_local
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
        ]

        tag_value_ops = [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.new_tag_value_view_name,
                target=OperationTarget.LOCAL,
            ),
            operations.RenameTable(
                storage_set=self.storage_set_key,
                old_table_name=self.new_tag_value_dist_table_name,
                new_table_name=self.old_tag_value_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.RenameTable(
                storage_set=self.storage_set_key,
                old_table_name=self.new_tag_value_local_table_name,
                new_table_name=self.old_tag_value_local_table_name,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.old_tag_value_view_name,
                columns=self.tag_value_table_columns,
                destination_table_name=self.old_tag_value_local_table_name,
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

        return meta_ops + tag_value_ops
