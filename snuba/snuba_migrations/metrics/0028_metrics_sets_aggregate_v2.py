from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import INT_TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.snuba_migrations.metrics.templates import COMMON_AGGR_COLUMNS
from snuba.utils.schemas import AggregateFunction, String


class Migration(migration.ClickhouseNodeMigrationLegacy):
    blocking = False
    table_name = "metrics_sets_v2_local"
    dist_table_name = "metrics_sets_v2_dist"
    granularity = "2048"
    aggregated_cols: Sequence[Column[Modifiers]] = [
        # This is a common column for aggregate tables going forward
        # but doesn't exist in the old templates so we include it here
        Column("use_case_id", String(Modifiers(low_cardinality=True))),
        *COMMON_AGGR_COLUMNS,
        Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
    ]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.METRICS,
                table_name=self.table_name,
                columns=self.aggregated_cols,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=StorageSetKey.METRICS,
                    order_by="(use_case_id, org_id, project_id, metric_id, granularity, timestamp, tags.key, tags.value, retention_days)",
                    primary_key="(use_case_id, org_id, project_id, metric_id, granularity, timestamp)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={"index_granularity": self.granularity},
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.METRICS,
                table_name=self.table_name,
                column=Column(
                    "_tags_hash",
                    Array(UInt(64), Modifiers(materialized=INT_TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags.value",
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.METRICS,
                table_name=self.table_name,
                index_name="bf_tags_hash",
                index_expression="_tags_hash",
                index_type="bloom_filter()",
                granularity=1,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.METRICS,
                table_name=self.table_name,
                index_name="bf_tags_key_hash",
                index_expression="tags.key",
                index_type="bloom_filter()",
                granularity=1,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [operations.DropTable(storage_set=StorageSetKey.METRICS, table_name=self.table_name)]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.METRICS,
                table_name=self.dist_table_name,
                columns=self.aggregated_cols,
                engine=table_engines.Distributed(
                    local_table_name=self.table_name, sharding_key=None
                ),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.METRICS,
                table_name=self.dist_table_name,
                column=Column(
                    "_tags_hash",
                    Array(UInt(64), Modifiers(materialized=INT_TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags.value",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(storage_set=StorageSetKey.METRICS, table_name=self.dist_table_name)
        ]
