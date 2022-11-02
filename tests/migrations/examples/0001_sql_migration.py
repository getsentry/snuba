from typing import Sequence

from snuba.clickhouse.columns import UUID, Array, Column, DateTime, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: Sequence[Column[Modifiers]] = [
    Column("request_id", UUID()),
    Column("projects", Array(UInt(64))),
    Column("organization", UInt(64, Modifiers(nullable=True))),
    Column("timestamp", DateTime()),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="dummy_querylog_local",
                columns=columns,
                engine=table_engines.MergeTree(
                    storage_set=StorageSetKey.QUERYLOG,
                    order_by="(toStartOfDay(timestamp), request_id)",
                    partition_by="(toMonday(timestamp))",
                    sample_by="request_id",
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="dummy_querylog_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="dummy_querylog_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="dummy_querylog_local",
                    sharding_key=None,
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="dummy_querylog_dist",
            )
        ]
