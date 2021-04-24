from typing import Sequence

from snuba.clickhouse.columns import (
    AggregateFunction,
    Array,
    Column,
    DateTime,
    Nested,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import INT_TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

sets_columns: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("metric_id", UInt(64)),
    Column("granularity", UInt(32)),
    Column("timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column("tags", Nested([Column("key", UInt(64)), Column("value", UInt(64))]),),
    Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
]

sets_mv_columns: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("metric_id", UInt(64)),
    Column("granularity", UInt(32)),
    Column("timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column("tags", Nested([Column("key", UInt(64)), Column("value", UInt(64))]),),
    Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
]


matview_statement = """
SELECT
    org_id,
    project_id,
    metric_id,
    60 as granularity,
    toStartOfMinute(timestamp) as timestamp,
    retention_days,
    tags.key,
    tags.value,
    uniqCombined64State(arrayJoin(set_values)) as value
FROM metrics_buckets_local
WHERE metric_type = 'set' AND materialization_version = 0
GROUP BY
    org_id,
    project_id,
    metric_id,
    tags.key,
    tags.value,
    timestamp,
    granularity,
    retention_days
"""


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.METRICS,
                table_name="metrics_sets_local",
                columns=sets_columns,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=StorageSetKey.METRICS,
                    order_by="(org_id, project_id, metric_id, granularity, timestamp)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={"index_granularity": "256"},
                ),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.METRICS,
                table_name="metrics_sets_local",
                column=Column(
                    "_tags_hash",
                    Array(UInt(64), Modifiers(materialized=INT_TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags.value",
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.METRICS,
                table_name="metrics_sets_local",
                index_name="bf_tags_hash",
                index_expression="_tags_hash",
                index_type="bloom_filter()",
                granularity=1,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.METRICS,
                table_name="metrics_sets_local",
                index_name="bf_tags_key_hash",
                index_expression="tags.key",
                index_type="bloom_filter()",
                granularity=1,
            ),
            operations.CreateMaterializedView(
                storage_set=StorageSetKey.METRICS,
                view_name="metrics_sets_minute_mv_local",
                destination_table_name="metrics_sets_local",
                columns=sets_mv_columns,
                query=matview_statement,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS,
                table_name="metrics_sets_minute_mv_local",
            ),
            operations.DropTable(
                storage_set=StorageSetKey.METRICS, table_name="metrics_sets_local",
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.METRICS,
                table_name="metrics_sets_dist",
                columns=sets_columns,
                engine=table_engines.Distributed(
                    local_table_name="metrics_sets_local", sharding_key=None
                ),
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS, table_name="metrics_sets_dist",
            ),
        ]
