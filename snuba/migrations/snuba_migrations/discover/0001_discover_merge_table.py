from typing import List, Sequence

from snuba.clickhouse.columns import (
    Column,
    DateTime,
    IPv4,
    IPv6,
    Nested,
    String,
    UInt,
    UUID,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: List[Column[Modifiers]] = [
    Column("event_id", UUID()),
    Column("project_id", UInt(64)),
    Column("type", String(Modifiers(low_cardinality=True))),
    Column("timestamp", DateTime()),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("release", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("dist", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("transaction_name", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("message", String(Modifiers(nullable=True))),
    Column("title", String(Modifiers(nullable=True))),
    Column("user", String(Modifiers(low_cardinality=True))),
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
    Column("contexts", Nested([("key", String()), ("value", String())])),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.DISCOVER,
                table_name="discover_local",
                columns=columns,
                engine=table_engines.Merge(
                    table_name_regex="^errors_local$|^transactions_local$"
                ),
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.DISCOVER, table_name="discover_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.DISCOVER,
                table_name="discover_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="discover_local", sharding_key=None,
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.DISCOVER, table_name="discover_dist"
            )
        ]
