from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("event_id", UUID()),
    Column("timestamp", DateTime()),
    Column("event_type", String(Modifiers(low_cardinality=True))),
    Column("user", String()),
    Column("details", String()),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.AUDIT_LOG,
                table_name="audit_log_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.AUDIT_LOG,
                    order_by="timestamp",
                    partition_by="user",
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.AUDIT_LOG,
                table_name="audit_log_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.AUDIT_LOG,
                table_name="audit_log_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="audit_log_local",
                    sharding_key=None,
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.AUDIT_LOG, table_name="audit_log_dist"
            )
        ]
