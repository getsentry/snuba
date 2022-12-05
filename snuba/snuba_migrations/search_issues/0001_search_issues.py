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

columns: List[Column[Modifiers]] = [
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("group_id", UInt(64)),
    Column("search_title", String()),
    Column("fingerprint", Array(String())),
    Column("occurrence_id", UUID()),
    Column("occurrence_type_id", UInt(8)),
    Column("detection_timestamp", DateTime()),
    Column("event_id", UUID(Modifiers(nullable=True))),
    Column("trace_id", UUID(Modifiers(nullable=True))),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("release", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("dist", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("receive_timestamp", DateTime()),
    Column("client_timestamp", DateTime()),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("user", String(Modifiers(default="''"))),
    Column("user_hash", UInt(64, Modifiers(materialized="cityHash64(user)"))),
    Column("user_id", String(Modifiers(nullable=True))),
    Column("user_name", String(Modifiers(nullable=True))),
    Column("user_email", String(Modifiers(nullable=True))),
    Column("ip_address_v4", IPv4(Modifiers(nullable=True))),
    Column("ip_address_v6", IPv6(Modifiers(nullable=True))),
    Column("sdk_name", String(Modifiers(low_cardinality=True, default="''"))),
    Column("sdk_version", String(Modifiers(low_cardinality=True, default="''"))),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("http_method", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("http_referer", String(Modifiers(nullable=True))),
    Column("retention_days", UInt(16)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    order_by="(organization_id, project_id, toStartOfDay(detection_timestamp))",
                    partition_by="(retention_days, toMonday(detection_timestamp))",
                    # TODO: need to figure out what sample is here
                    # sample_by="cityHash64(occurrence_id)",
                    settings={"index_granularity": "8192"},
                    storage_set=StorageSetKey.SEARCH_ISSUES,
                    ttl="detection_timestamp + toIntervalDay(retention_days)",
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_local",
            )
        ]

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="search_issues_local",
                    sharding_key="project_id",
                ),
            )
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_dist",
            )
        ]
