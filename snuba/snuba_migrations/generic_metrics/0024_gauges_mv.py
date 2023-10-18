from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime, Nested, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import AggregateFunction


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    view_name = "generic_metric_gauges_aggregation_mv"
    dest_table_name = "generic_metric_gauges_aggregated_local"
    storage_set_key = StorageSetKey.GENERIC_METRICS_GAUGES
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
                storage_set=self.storage_set_key,
                view_name=self.view_name,
                columns=self.dest_table_columns,
                destination_table_name=self.dest_table_name,
                target=OperationTarget.LOCAL,
                query="""
                SELECT
                    use_case_id,
                    org_id,
                    project_id,
                    metric_id,
                    arrayJoin(granularities) as granularity,
                    tags.key,
                    tags.indexed_value,
                    tags.raw_value,
                    timestamp as raw_timestamp,
                    toDateTime(multiIf(granularity=0,10,granularity=1,60,granularity=2,3600,granularity=3,86400,-1) *
                      intDiv(toUnixTimestamp(raw_timestamp),
                             multiIf(granularity=0,10,granularity=1,60,granularity=2,3600,granularity=3,86400,-1))) as _timestamp,
                    retention_days,
                    minState(arrayJoin(gauges_values.min)) as min,
                    maxState(arrayJoin(gauges_values.max)) as max,
                    avgState(arrayJoin(gauges_values.avg)) as avg,
                    sumState(arrayJoin(gauges_values.sum)) as sum,
                    countState(arrayJoin(gauges_values.count)) as count,
                    argMaxState(arrayJoin(gauges_values.last), raw_timestamp) as last
                FROM generic_metric_gauges_raw_local
                WHERE materialization_version = 1
                  AND metric_type = 'gauge'
                GROUP BY
                    use_case_id,
                    org_id,
                    project_id,
                    metric_id,
                    tags.key,
                    tags.indexed_value,
                    tags.raw_value,
                    raw_timestamp,
                    granularity,
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
