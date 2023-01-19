from typing import Sequence

from snuba.clickhouse.columns import UUID, Array, Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

table_name = "querylog_local"


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds codecs to columns and updates tables settings to match what exists in SaaS
    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:

        return [
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column("request_id", UUID(Modifiers(codecs=["NONE"]))),
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
            operations.ModifyTableSettings(
                StorageSetKey.QUERYLOG,
                table_name,
                {"min_bytes_for_wide_part": "10000000", "ttl_only_drop_parts": 1},
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            # Resetting the codecs to the default values is not supported by Clickhouse 20
            # so the column modifications are no-ops. This can be changed once we upgrade to 21
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column("request_id", UUID()),
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
            operations.ModifyTableSettings(
                StorageSetKey.QUERYLOG,
                table_name,
                {"min_bytes_for_wide_part": 0, "ttl_only_drop_parts": 0},
                target=operations.OperationTarget.LOCAL,
            ),
        ]
