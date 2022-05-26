from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: List[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("profile_id", UUID()),
    Column("transaction_id", UUID()),
    Column("trace_id", UUID()),
    Column("received", DateTime()),
    # filtering data
    Column("android_api_level", UInt(32)),
    Column("device_classification", String(Modifiers(low_cardinality=True))),
    Column("device_locale", String(Modifiers(low_cardinality=True))),
    Column("device_manufacturer", String(Modifiers(low_cardinality=True))),
    Column("device_model", String(Modifiers(low_cardinality=True))),
    Column("device_os_build_number", String(Modifiers(low_cardinality=True))),
    Column("device_os_name", String(Modifiers(low_cardinality=True))),
    Column("device_os_version", String(Modifiers(low_cardinality=True))),
    Column("duration_ns", UInt(64)),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("transaction_name", String(Modifiers(low_cardinality=True))),
    Column("version_name", String()),
    Column("version_code", String()),
    # function data
    Column("function_id", UInt(64)),
    Column("symbol", String()),
    Column("address", UInt(64)),
    Column("thread_id", UInt(64)),
    Column("filename", String()),
    Column("line", UInt(32)),
    Column("self_time", UInt(64)),
    Column("duration", UInt(64)),
    Column("timestamp", UInt(64)),
    Column("fingerprint", UInt(64)),
    Column("parent_fingerprint", UInt(64)),
    # internal data
    Column("retention_days", UInt(16)),
    Column("partition", UInt(16)),
    Column("offset", UInt(64)),
    Column("deleted", UInt(8)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.PROFILES,
                table_name="functions_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    order_by="(org_id, project_id, toStartOfDay(received), transaction_name, cityHash64(profile_id), cityHash64(function_id))",
                    partition_by="(retention_days, toMonday(received))",
                    sample_by="cityHash64(profile_id)",
                    settings={"index_granularity": "8192"},
                    storage_set=StorageSetKey.PROFILES,
                    ttl="received + toIntervalDay(retention_days)",
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.PROFILES,
                table_name="functions_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.PROFILES,
                table_name="functions_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="functions_local",
                    sharding_key="cityHash64(profile_id)",
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.PROFILES,
                table_name="functions_dist",
            )
        ]
