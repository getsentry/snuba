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
from snuba.utils.schemas import Float


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    view_name = "generic_metric_counters_aggregation_mv"
    dest_table_name = "generic_metric_counters_aggregated_local"
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
                    ("raw_value", String()),
                ]
            ),
        ),
        Column("value", AggregateFunction("sum", [Float(64)])),
        Column("use_case_id", String(Modifiers(low_cardinality=True))),
    ]
    storage_set_key = StorageSetKey.GENERIC_METRICS_COUNTERS

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.view_name,
                columns=self.dest_table_columns,
                destination_table_name=self.dest_table_name,
                query="""
                SELECT
                    use_case_id,
                    org_id,
                    project_id,
                    metric_id,
                    arrayJoin(granularities) as granularity,
                    tags.key,
                    tags.raw_value,
                    toDateTime(multiIf(granularity=0,10,granularity=1,60,granularity=2,3600,granularity=3,86400,-1) *
                      intDiv(toUnixTimestamp(timestamp),
                             multiIf(granularity=0,10,granularity=1,60,granularity=2,3600,granularity=3,86400,-1))) as timestamp,
                    retention_days,
                    sumState(count_value) as value
                FROM generic_metric_counters_raw_local
                WHERE materialization_version = 1
                  AND metric_type = 'counter'
                GROUP BY
                    use_case_id,
                    org_id,
                    project_id,
                    metric_id,
                    tags.key,
                    tags.raw_value,
                    timestamp,
                    granularity,
                    retention_days
                """,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.view_name,
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
