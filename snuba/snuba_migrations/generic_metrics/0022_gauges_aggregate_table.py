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
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import Float


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    granularity = "2048"
    local_table_name = "generic_metric_gauges_aggregated_local"
    dist_table_name = "generic_metric_gauges_aggregated_dist"
    storage_set_key = StorageSetKey.GENERIC_METRICS_GAUGES
    columns: Sequence[Column[Modifiers]] = [
        Column("org_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("metric_id", UInt(64)),
        Column("granularity", UInt(8)),
        Column(
            "rounded_timestamp", DateTime(modifiers=Modifiers(codecs=["DoubleDelta"]))
        ),
        Column("last_timestamp", AggregateFunction("max", [DateTime()])),
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
        Column("min", AggregateFunction("min", [Float(64)])),
        Column("max", AggregateFunction("max", [Float(64)])),
        Column("avg", AggregateFunction("avg", [Float(64)])),
        Column("sum", AggregateFunction("sum", [Float(64)])),
        Column("count", AggregateFunction("sum", [UInt(64)])),
        Column(
            "last",
            AggregateFunction(
                "argMax",
                [Float(64), DateTime()],
            ),
        ),
    ]

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    order_by="(org_id, project_id, metric_id, granularity, "
                    "rounded_timestamp, tags.key, tags.indexed_value, "
                    "tags.raw_value, retention_days, use_case_id)",
                    primary_key="(org_id, project_id, metric_id, granularity, rounded_timestamp)",
                    partition_by="(retention_days, toMonday(rounded_timestamp))",
                    settings={"index_granularity": self.granularity},
                    ttl="rounded_timestamp + toIntervalDay(retention_days)",
                ),
                columns=self.columns,
                target=OperationTarget.LOCAL,
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
                target=OperationTarget.LOCAL,
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
                target=OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name="bf_indexed_tags_hash",
                index_expression="_indexed_tags_hash",
                index_type="bloom_filter()",
                granularity=1,
                target=OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name="bf_raw_tags_hash",
                index_expression="_raw_tags_hash",
                index_type="bloom_filter()",
                granularity=1,
                target=OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                index_name="bf_tags_key_hash",
                index_expression="tags.key",
                index_type="bloom_filter()",
                granularity=1,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.local_table_name, sharding_key=None
                ),
                columns=self.columns,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                target=OperationTarget.LOCAL,
            ),
        ]
