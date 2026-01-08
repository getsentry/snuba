from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: List[Column[Modifiers]] = [
    # primary key
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("transaction_id", UUID()),
    Column("profile_id", UUID()),
    Column("received", DateTime()),
    # profiling data
    Column("profile", String(Modifiers(codecs=["LZ4HC(9)"]))),
    # filtering data
    Column("android_api_level", UInt(32, Modifiers(nullable=True))),
    Column("device_classification", String(Modifiers(low_cardinality=True))),
    Column("device_locale", String(Modifiers(low_cardinality=True))),
    Column("device_manufacturer", String(Modifiers(low_cardinality=True))),
    Column("device_model", String(Modifiers(low_cardinality=True))),
    Column(
        "device_os_build_number", String(Modifiers(low_cardinality=True, nullable=True))
    ),
    Column("device_os_name", String(Modifiers(low_cardinality=True))),
    Column("device_os_version", String(Modifiers(low_cardinality=True))),
    Column("duration_ns", UInt(64)),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("trace_id", UUID()),
    Column("transaction_name", String(Modifiers(low_cardinality=True))),
    Column("version_name", String()),
    Column("version_code", String()),
    # internal data
    Column("retention_days", UInt(16)),
    Column("partition", UInt(16)),
    Column("offset", UInt(64)),
]


class Migration(migration.ClickhouseNodeMigrationLegacy):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.PROFILES,
                table_name="profiles_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    order_by="(organization_id, project_id, toStartOfDay(received), cityHash64(profile_id))",
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
                table_name="profiles_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.PROFILES,
                table_name="profiles_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="profiles_local",
                    sharding_key="cityHash64(profile_id)",
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.PROFILES,
                table_name="profiles_dist",
            )
        ]
