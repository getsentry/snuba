from typing import Sequence

from snuba.clickhouse.columns import Array, Column, DateTime, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

table_name = "querylog_local"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column("request_id", String()),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column("request_body", String()),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column("timestamp", DateTime()),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column(
                    "clickhouse_queries.sql",
                    Array(String()),
                ),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column(
                    "clickhouse_queries.stats",
                    Array(String()),
                ),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropIndex(
                StorageSetKey.QUERYLOG,
                table_name,
                "bf_projects",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifySortingKey(
                StorageSetKey.QUERYLOG,
                table_name,
                ["toStartOfDay(timestamp)", "request_id"],
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifySampleBy(
                StorageSetKey.QUERYLOG,
                table_name,
                "request_id",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ResetTableSettings(
                StorageSetKey.QUERYLOG,
                table_name,
                ["min_bytes_for_wide_part", "ttl_only_drop_parts"],
            ),
        ]

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:

        return [
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column("request_id", String(Modifiers(codecs=["NONE"]))),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column("request_body", String(Modifiers(codecs=["LZ4HC(0)"]))),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column("timestamp", DateTime(Modifiers(codecs=["T64", "ZSTD(1)"]))),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column(
                    "clickhouse_queries.sql",
                    Array(String(Modifiers(codecs=["LZ4HC(0)"]))),
                ),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column(
                    "clickhouse_queries.stats",
                    Array(String(Modifiers(codecs=["LZ4HC(0)"]))),
                ),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                StorageSetKey.QUERYLOG,
                table_name,
                "bf_projects",
                "projects",
                "bloom_filter",
                1,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifySortingKey(
                StorageSetKey.QUERYLOG,
                table_name,
                ["dataset", "referrer", "toStartOfDay(timestamp)", "request_id"],
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifySampleBy(
                StorageSetKey.QUERYLOG,
                table_name,
                "request_id TTL timestamp + toIntervalDay(30)",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifyTableSettings(
                StorageSetKey.QUERYLOG,
                table_name,
                {"min_bytes_for_wide_part": "10000000", "ttl_only_drop_parts": 1},
            ),
        ]


# clickhouse_queries.stats` Array(String) CODEC(LZ4HC(0))
# INDEX bf_projects projects TYPE bloom_filter GRANULARITY 1
# ORDER BY (dataset, referrer

#  SAMPLE BY request_id TTL timestamp + toIntervalDay(30)

#         `` Array(String) CODEC()
# return [operations.ModifyColumn()]
