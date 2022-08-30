from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

UNKNOWN_SPAN_STATUS = 2


columns: List[Column[Modifiers]] = [
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
                    order_by="(timestamp, cityHash64(event_id))",
                    partition_by="(user, event_type)",
                    sample_by="cityHash64(event_id)",
                    ttl="timestamp + toIntervalDay(360)",  # keep for 360 days
                    settings={"index_granularity": "8192"},
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
                    sharding_key="cityHash64(event_id)",
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="audit_log_dist",
            )
        ]
