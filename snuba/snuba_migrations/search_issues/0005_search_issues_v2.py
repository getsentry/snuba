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
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

columns: List[Column[Modifiers]] = [
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("group_id", UInt(64)),
    Column("search_title", String()),
    Column("primary_hash", UUID()),
    Column("fingerprint", Array(String())),
    Column("occurrence_id", UUID()),
    Column("occurrence_type_id", UInt(16)),
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
    Column("user", String(Modifiers(nullable=True))),
    Column(
        "user_hash", UInt(64, Modifiers(nullable=True, materialized="cityHash64(user)"))
    ),
    Column("user_id", String(Modifiers(nullable=True))),
    Column("user_name", String(Modifiers(nullable=True))),
    Column("user_email", String(Modifiers(nullable=True))),
    Column("ip_address_v4", IPv4(Modifiers(nullable=True))),
    Column("ip_address_v6", IPv6(Modifiers(nullable=True))),
    Column("sdk_name", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("sdk_version", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("http_method", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("http_referer", String(Modifiers(nullable=True))),
    Column("deleted", UInt(8)),
    Column("message_timestamp", DateTime()),
    Column("partition", UInt(16)),
    Column("offset", UInt(64)),
    Column("retention_days", UInt(16)),
]


class Migration(migration.ClickhouseNodeMigration):
    """
    This migration rebuilds the search_issues table to use `client_timestamp` as part of the primary key instead of
    the previous `receive_timestamp`.

    """

    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            # local first
            operations.CreateTable(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_local_v2",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    order_by="(project_id, toStartOfDay(client_timestamp), primary_hash, cityHash64(occurrence_id))",
                    version_column="deleted",
                    partition_by="(retention_days, toMonday(client_timestamp))",
                    sample_by="cityHash64(occurrence_id)",
                    settings={"index_granularity": "8192"},
                    storage_set=StorageSetKey.SEARCH_ISSUES,
                    ttl="client_timestamp + toIntervalDay(retention_days)",
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_local_v2",
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags.value",
                target=OperationTarget.LOCAL,
            ),
            # dist second
            operations.CreateTable(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_dist_v2",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="search_issues_local_v2",
                    sharding_key="cityHash64(occurrence_id)",
                ),
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name="search_issues_dist_v2",
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags.value",
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name=params[0],
                target=params[1],
            )
            for params in [
                ("search_issues_dist_v2", OperationTarget.DISTRIBUTED),
                ("search_issues_local_v2", OperationTarget.LOCAL),
            ]
        ]
