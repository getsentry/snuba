from typing import Sequence

from snuba.clickhouse.columns import (
    Column,
    DateTime,
    IPv4,
    IPv6,
    Nested,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.migrations.columns import LowCardinality  # TODO: verify
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines


columns = [
    Column("event_id", UUID()),
    Column("project_id", UInt(64)),
    Column("type", LowCardinality(String())),
    Column("timestamp", DateTime()),
    Column("platform", LowCardinality(String())),
    Column("environment", LowCardinality(Nullable(String()))),
    Column("release", LowCardinality(Nullable(String()))),
    Column("dist", LowCardinality(Nullable(String()))),
    Column("transaction_name", LowCardinality(Nullable(String()))),
    Column("message", Nullable(String())),
    Column("title", Nullable(String())),
    Column("user", LowCardinality(String())),
    Column("user_hash", UInt(64)),
    Column("user_id", Nullable(String())),
    Column("user_name", Nullable(String())),
    Column("user_email", Nullable(String())),
    Column("ip_address_v4", Nullable(IPv4())),
    Column("ip_address_v6", Nullable(IPv6())),
    Column("sdk_name", LowCardinality(Nullable(String()))),
    Column("sdk_version", LowCardinality(Nullable(String()))),
    Column("http_method", LowCardinality(Nullable(String()))),
    Column("http_referer", Nullable(String())),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("contexts", Nested([("key", String()), ("value", String())])),
]


class Migration(migration.MultiStepMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
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

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.DISCOVER, table_name="discover_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
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

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.DISCOVER, table_name="discover_dist"
            )
        ]
