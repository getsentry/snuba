from typing import List, Sequence

from snuba.clickhouse.columns import (
    UUID,
    Column,
    DateTime,
    IPv4,
    IPv6,
    Nested,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

columns: List[Column[Modifiers]] = [
    Column("event_id", UUID()),
    Column("project_id", UInt(64)),
    Column("type", String(Modifiers(low_cardinality=True))),
    Column("timestamp", DateTime()),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("release", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("dist", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("transaction_name", String(Modifiers(low_cardinality=True))),
    Column("message", String()),
    Column("title", String()),
    Column("user", String()),
    Column("user_hash", UInt(64)),
    Column("user_id", String(Modifiers(nullable=True))),
    Column("user_name", String(Modifiers(nullable=True))),
    Column("user_email", String(Modifiers(nullable=True))),
    Column("ip_address_v4", IPv4(Modifiers(nullable=True))),
    Column("ip_address_v6", IPv6(Modifiers(nullable=True))),
    Column("sdk_name", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("sdk_version", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("http_method", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("http_referer", String(Modifiers(nullable=True))),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("_tags_hash_map", UInt(64)),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("trace_id", UUID(Modifiers(nullable=True))),
    Column("span_id", UInt(64, Modifiers(nullable=True))),
    Column("deleted", UInt(8)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = True

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.DISCOVER,
                table_name="discover_dist",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.DISCOVER,
                table_name="discover_dist",
                columns=columns,
                target=OperationTarget.DISTRIBUTED,
                engine=table_engines.Merge(
                    table_name_regex="^errors_dist$|^transactions_dist$"
                ),
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.DISCOVER,
                table_name="discover_dist",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.DISCOVER,
                table_name="discover_dist",
                columns=columns,
                target=OperationTarget.DISTRIBUTED,
                engine=table_engines.Distributed(
                    local_table_name="discover_local", sharding_key=None,
                ),
            ),
        ]
