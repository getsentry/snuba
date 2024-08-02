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
    view_name = "generic_metric_distributions_aggregation_mv_v4"
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
    storage_set_key = StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.view_name,
                columns=self.dest_table_columns,
                destination_table_name="generic_metric_distributions_aggregated_local",
                target=operations.OperationTarget.LOCAL,
                query="""
                SELECT
                    org_id,
                    project_id,
                    metric_id,
                    arrayJoin(granularities) AS granularity,
                    toDateTime(multiIf(granularity = 0, 10, granularity = 1, 60, granularity = 2, 3600, granularity = 3, 86400, -1) * intDiv(toUnixTimestamp(timestamp), multiIf(granularity = 0, 10, granularity = 1, 60, granularity = 2, 3600, granularity = 3, 86400, -1))) AS timestamp,
                    least(retention_days, multiIf(granularity = 0, decasecond_retention_days, granularity = 1, min_retention_days, granularity = 2, hr_retention_days, granularity = 3, day_retention_days, 0)) AS retention_days,
                    tags.key,
                    tags.indexed_value,
                    tags.raw_value,
                    use_case_id,
                    quantilesStateIf(0.5, 0.75, 0.9, 0.95, 0.99)(arrayJoin(distribution_values) AS values_rows, disable_percentiles = 0) AS percentiles,
                    quantilesTDigestWeightedStateIf(0.5, 0.75, 0.9, 0.95, 0.99)(values_rows, sampling_weight, disable_percentiles = 0) AS percentiles_weighted,
                    minState(values_rows) AS min,
                    maxState(values_rows) AS max,
                    avgState(values_rows) as avg,
                    avgWeightedState(values_rows, sampling_weight) AS avg_weighted,
                    sumState(values_rows) AS sum,
                    sumState(values_rows * sampling_weight) AS sum_weighted,
                    countState(values_rows) AS count,
                    sumState(sampling_weight) AS count_weighted,
                    histogramStateIf(250)(values_rows, (disable_percentiles = 0) AND enable_histogram) AS histogram_buckets
                FROM generic_metric_distributions_raw_local
                WHERE (materialization_version = 4) AND (metric_type = 'distribution')
                GROUP BY
                    org_id,
                    project_id,
                    metric_id,
                    granularity,
                    timestamp,
                    retention_days,
                    tags.key,
                    tags.indexed_value,
                    tags.raw_value,
                    use_case_id;
                """,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.view_name,
                target=operations.OperationTarget.LOCAL,
            )
        ]
