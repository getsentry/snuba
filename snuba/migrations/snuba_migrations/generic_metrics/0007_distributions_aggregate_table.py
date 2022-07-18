from typing import Sequence

from snuba.clickhouse.columns import (
    AggregateFunction,
    Array,
    Column,
    DateTime,
    Nested,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import (
    hash_map_int_column_definition,
    hash_map_int_key_str_value_column_definition,
)
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.utils.schemas import Float


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    granularity = "2048"
    local_table_name = "generic_metric_distributions_aggregated_local"
    dist_table_name = "generic_metric_distributions_aggregated_dist"
    columns: Sequence[Column[Modifiers]] = [
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
        Column("use_case_id", String(Modifiers(low_cardinality=True))),
        Column(
            "percentiles",
            AggregateFunction("quantiles(0.5, 0.75, 0.9, 0.95, 0.99)", [Float(64)]),
        ),
        Column("min", AggregateFunction("min", [Float(64)])),
        Column("max", AggregateFunction("max", [Float(64)])),
        Column("avg", AggregateFunction("avg", [Float(64)])),
        Column("sum", AggregateFunction("sum", [Float(64)])),
        Column("count", AggregateFunction("count", [Float(64)])),
        Column("histogram_buckets", AggregateFunction("histogram(250)", [Float(64)])),
    ]
    storage_set_key = StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    order_by="(org_id, project_id, metric_id, granularity, timestamp, tags.key, tags.indexed_value, tags.raw_value, retention_days, use_case_id)",
                    primary_key="(org_id, project_id, metric_id, granularity, timestamp)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={"index_granularity": self.granularity},
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                columns=self.columns,
            ),
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column(
                    "_indexed_tags_hash",
                    Array(
                        UInt(64),
                        Modifiers(
                            materialized=hash_map_int_column_definition(
                                "tags.key", "tags.indexed_value"
                            )
                        ),
                    ),
                ),
            ),
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column(
                    "_raw_tags_hash",
                    Array(
                        UInt(64),
                        Modifiers(
                            materialized=hash_map_int_key_str_value_column_definition(
                                "tags.key", "tags.raw_value"
                            )
                        ),
                    ),
                ),
            ),
            operations.AddIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name="bf_indexed_tags_hash",
                index_expression="_indexed_tags_hash",
                index_type="bloom_filter()",
                granularity=1,
            ),
            operations.AddIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name="bf_raw_tags_hash",
                index_expression="_raw_tags_hash",
                index_type="bloom_filter()",
                granularity=1,
            ),
            operations.AddIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name="bf_tags_key_hash",
                index_expression="tags.key",
                index_type="bloom_filter()",
                granularity=1,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.local_table_name, sharding_key=None
                ),
                columns=self.columns,
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
            )
        ]
