from typing import Sequence

from snuba.clickhouse.columns import (
    AggregateFunction,
    Column,
    DateTime,
    Nested,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    view_name = "generic_metric_sets_aggregation_mv_v2"
    dest_table_columns: Sequence[Column[Modifiers]] = [
        Column("org_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("metric_id", UInt(64)),
        Column("granularity", UInt(8)),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["DoubleDelta"]))),
        Column("retention_days", UInt(16)),
        Column(
            "tags",
            Nested(
                [
                    ("key", UInt(64)),
                    ("indexed_value", UInt(64)),
                    ("raw_value", String()),
                ]
            ),
        ),
        Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
        Column("use_case_id", String(Modifiers(low_cardinality=True))),
    ]

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                view_name=self.view_name,
                columns=self.dest_table_columns,
                destination_table_name="generic_metric_sets_local",
                target=operations.OperationTarget.LOCAL,
                query="""
                SELECT
                    use_case_id,
                    org_id,
                    project_id,
                    metric_id,
                    arrayJoin([0,1,2,3]) as granularity,
                    tags.key,
                    tags.indexed_value,
                    tags.raw_value,
                    toDateTime(multiIf(granularity=0,10,granularity=1,60,granularity=2,3600,granularity=3,86400,-1) *
                      intDiv(toUnixTimestamp(timestamp),
                             multiIf(granularity=0,10,granularity=1,60,granularity=2,3600,granularity=3,86400,-1))) as timestamp,
                    least(retention_days,
                        multiIf(granularity=0,decasecond_retention_days,
                                granularity=1,min_retention_days,
                                granularity=2,hr_retention_days,
                                granularity=3,day_retention_days,
                                0)) as retention_days,
                    uniqCombined64State(arrayJoin(set_values)) as value
                FROM generic_metric_sets_raw_local
                WHERE materialization_version = 2
                  AND metric_type = 'set'
                GROUP BY
                    use_case_id,
                    org_id,
                    project_id,
                    metric_id,
                    tags.key,
                    tags.indexed_value,
                    tags.raw_value,
                    timestamp,
                    granularity,
                    retention_days
                """,
            )
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                table_name=self.view_name,
                target=operations.OperationTarget.LOCAL,
            )
        ]
