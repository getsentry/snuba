from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, NamedTuple, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: List[Column[Modifiers]] = [
    # primary key
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("transaction_id", UUID()),
    Column("received", DateTime()),
    # profiling data
    Column("profile", String(Modifiers(codecs=["LZ4HC(9)"]))),
    Column("symbols", String(Modifiers(codecs=["LZ4HC(9)"]))),
    # filtering data
    Column("android_api_level", UInt(32, Modifiers(nullable=True))),
    Column("device_classification", String(Modifiers(low_cardinality=True))),
    Column("device_locale", String(Modifiers(low_cardinality=True))),
    Column("device_manufacturer", String(Modifiers(low_cardinality=True))),
    Column("device_model", String(Modifiers(low_cardinality=True))),
    Column("device_os_build_number", String(Modifiers(low_cardinality=True))),
    Column("device_os_name", String(Modifiers(low_cardinality=True))),
    Column("device_os_version", String(Modifiers(low_cardinality=True))),
    Column("duration_ns", UInt(64)),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("error_code", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("error_description", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("trace_id", UUID()),
    Column("transaction_name", String(Modifiers(low_cardinality=True))),
    Column("version", NamedTuple((("name", String()), ("code", String())))),
    # internal data
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
    Column("partition", UInt(16)),
    Column("offset", UInt(16)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.PROFILES,
                table_name="profiles_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.PROFILES,
                    order_by="(organization_id, project_id, transaction_id)",
                    ttl="received + toIntervalDay(retention_days)",
                    settings={"index_granularity": "8192"},
                    version_column="deleted",
                    partition_by="(organization_id, project_id, toMonday(received))",
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.PROFILES, table_name="profiles_local",
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
                    sharding_key="cityHash64(transaction_id)",
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.PROFILES, table_name="profiles_dist",
            )
        ]
