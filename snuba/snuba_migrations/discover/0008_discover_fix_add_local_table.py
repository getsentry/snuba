from typing import List, Sequence

from snuba.clickhouse.columns import (
    UUID,
    Array,
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
    Column("_tags_hash_map", Array(UInt(64))),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("span_id", UInt(64, Modifiers(nullable=True))),
    Column("trace_id", UUID(Modifiers(nullable=True))),
    Column("deleted", UInt(8)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    local_table_name = "discover_local"

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.DISCOVER,
                table_name=self.local_table_name,
                columns=columns,
                engine=table_engines.Merge(
                    table_name_regex="^errors_local$|^transactions_local$"
                ),
                target=OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        # we do not drop the local table because it is created in previous migrations
        return []
