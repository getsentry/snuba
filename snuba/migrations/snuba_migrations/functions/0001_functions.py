from typing import List, Sequence

from snuba.clickhouse.columns import (
    UUID,
    AggregateFunction,
    Array,
    Column,
    DateTime,
    Float,
    Nested,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

common_columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("transaction_name", String()),
    Column("timestamp", DateTime()),
    Column("depth", UInt(32)),
    Column("parent_fingerprint", UInt(64)),
    Column("fingerprint", UInt(64)),
    Column("symbol", String()),
    Column("image", String()),
    Column("filename", String()),
    Column("is_application", UInt(8)),
    Column("tags", Nested([Column("key", String()), Column("value", String())])),
    Column("retention_days", UInt(16)),
]

raw_columns: List[Column[Modifiers]] = common_columns + [
    Column("durations", Array(Float(64))),
    Column("profile_id", UUID()),
]

agg_columns: List[Column[Modifiers]] = common_columns + [
    Column("count", AggregateFunction("count", [Float(64)])),
    Column(
        "percentiles",
        AggregateFunction("quantiles(0.5, 0.75, 0.9, 0.95, 0.99)", [Float(64)]),
    ),
    Column("min", AggregateFunction("min", [Float(64)])),
    Column("max", AggregateFunction("max", [Float(64)])),
    Column("avg", AggregateFunction("avg", [Float(64)])),
    Column("sum", AggregateFunction("sum", [Float(64)])),
    Column("worst", AggregateFunction("argMax", [UUID(), Float(64)])),
    Column("examples", AggregateFunction("groupUniqArray(5)", [UUID()])),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    index_granularity = "2048"
    storage_set = StorageSetKey.FUNCTIONS

    data_granularity = 60 * 60  # 1 hour buckets

    local_raw_table = "functions_raw_local"
    dist_raw_table = "functions_raw_dist"

    local_materialized_table = "functions_mv_local"
    dist_materialized_table = "functions_mv_dist"

    local_view_table = "functions_local"

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set,
                table_name=self.local_raw_table,
                columns=raw_columns,
                engine=table_engines.MergeTree(
                    storage_set=self.storage_set,
                    order_by="(project_id, transaction_name, timestamp)",
                    partition_by="(toStartOfInterval(timestamp, INTERVAL 12 HOUR))",
                    ttl="timestamp + toIntervalDay(1)",
                ),
            ),
            operations.CreateTable(
                storage_set=self.storage_set,
                table_name=self.local_materialized_table,
                columns=agg_columns,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set,
                    order_by="(project_id, transaction_name, timestamp, depth, parent_fingerprint, fingerprint, symbol, image, filename, is_application, tags.key, tags.value, retention_days)",
                    primary_key="(project_id, transaction_name, timestamp, depth, parent_fingerprint, fingerprint)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={"index_granularity": self.index_granularity},
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
            ),
            operations.AddColumn(
                storage_set=self.storage_set,
                table_name=self.local_materialized_table,
                column=Column(
                    "_tags_hash",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags.value",
            ),
            operations.AddIndex(
                storage_set=self.storage_set,
                table_name=self.local_materialized_table,
                index_name="bf_tags_hash",
                index_expression="_tags_hash",
                index_type="bloom_filter()",
                granularity=1,
            ),
            operations.AddIndex(
                storage_set=self.storage_set,
                table_name=self.local_materialized_table,
                index_name="bf_tags__key_hash",
                index_expression="tags.key",
                index_type="bloom_filter()",
                granularity=1,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set,
                view_name=self.local_view_table,
                destination_table_name=self.local_materialized_table,
                columns=agg_columns,
                query=self.__MATVIEW_STATEMENT,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set, table_name=self.local_raw_table
            ),
            operations.DropTable(
                storage_set=self.storage_set, table_name=self.local_materialized_table
            ),
            operations.DropTable(
                storage_set=self.storage_set, table_name=self.local_view_table
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set,
                table_name=self.dist_raw_table,
                columns=raw_columns,
                engine=table_engines.Distributed(
                    local_table_name=self.local_raw_table,
                    sharding_key=None,
                ),
            ),
            operations.CreateTable(
                storage_set=self.storage_set,
                table_name=self.dist_materialized_table,
                columns=agg_columns,
                engine=table_engines.Distributed(
                    local_table_name=self.local_materialized_table,
                    sharding_key=None,
                ),
            ),
            operations.AddColumn(
                storage_set=self.storage_set,
                table_name=self.dist_materialized_table,
                column=Column(
                    "_tags_hash",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags.value",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set, table_name=self.dist_raw_table
            ),
            operations.DropTable(
                storage_set=self.storage_set, table_name=self.dist_materialized_table
            ),
        ]

    @property
    def __MATVIEW_STATEMENT(self) -> str:
        return f"""
            SELECT
                project_id,
                transaction_name,
                symbol,
                image,
                filename,
                fingerprint,
                parent_fingerprint,
                depth,
                is_application,
                tags.key,
                tags.value,
                toDateTime({self.data_granularity} * intDiv(toUnixTimestamp(timestamp), {self.data_granularity})) AS timestamp,
                retention_days,
                countState(arrayJoin(durations) AS duration) AS count,
                quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(duration) AS percentiles,
                minState(duration) AS min,
                maxState(duration) AS max,
                avgState(duration) AS avg,
                sumState(duration) AS sum,
                argMaxState(profile_id, duration) as worst,
                groupUniqArrayState(5)(profile_id) as examples
            FROM {self.local_raw_table}
            GROUP BY
                project_id,
                transaction_name,
                symbol,
                image,
                filename,
                fingerprint,
                parent_fingerprint,
                depth,
                is_application,
                tags.key,
                tags.value,
                timestamp,
                retention_days
        """
