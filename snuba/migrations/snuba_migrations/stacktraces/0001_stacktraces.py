from typing import List, Sequence

from snuba.clickhouse.columns import (
    UUID,
    Column,
    DateTime,
    String,
    UInt,
    NamedTuple,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: List[Column[Modifiers]] = [
    # primary key
    Column("project_id", UInt(64)),
    Column("transaction_id", UUID()),

    # profiling data
    Column("stacktrace", String(Modifiers(codecs=["LZ4HC(9)"]))),
    Column("symbols", String(Modifiers(codecs=["LZ4HC(9)"]))),

    # filtering data
    Column("trace_id", UUID()),
    Column("transaction_name", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),

    Column("version", NamedTuple((("name", String()), ("code", String())))),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("android_api_level", UInt(32, Modifiers(nullable=True))),
    Column("device_classification", String(Modifiers(low_cardinality=True))),
    Column("device_locale", String(Modifiers(low_cardinality=True))),
    Column("device_manufacturer", String(Modifiers(low_cardinality=True))),
    Column("device_model", String(Modifiers(low_cardinality=True))),
    Column("device_os_build_number", String(Modifiers(low_cardinality=True))),
    Column("device_os_name", String(Modifiers(low_cardinality=True))),
    Column("device_os_version", String(Modifiers(low_cardinality=True))),

    Column("error_code", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("error_description", String(Modifiers(low_cardinality=True, nullable=True))),

    Column("duration_ns", UInt(64)),
    Column("ingested_at_ts", DateTime()),

    # internal data
    Column("retention_days", UInt(16)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.STACKTRACES,
                table_name="stacktraces_local",
                columns=columns,
                engine=table_engines.MergeTree(
                    storage_set=StorageSetKey.STACKTRACES,
                    order_by="(project_id, transaction_id, ingested_at_ts)",
                    ttl="finish_ts + toIntervalDay(retention_days)",
                    settings={"index_granularity": "8192"},
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.STACKTRACES, table_name="stacktraces_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.STACKTRACES,
                table_name="stacktraces_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="stacktraces_local",
                    sharding_key="cityHash64(project_id, transaction_name)",
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.STACKTRACES, table_name="stacktraces_dist",
            )
        ]
