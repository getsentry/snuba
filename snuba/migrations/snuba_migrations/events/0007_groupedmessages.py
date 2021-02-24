from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: Sequence[Column[Modifiers]] = [
    # Kafka topic offset
    Column("offset", UInt(64)),
    # GroupStatus in Sentry does not have a 'DELETED' state that reflects the deletion
    # of the record. Having a dedicated clickhouse-only flag to identify this case seems
    # more consistent than add an additional value into the status field below that does not
    # exists on the Sentry side.
    Column("record_deleted", UInt(8)),
    # PG columns
    Column("project_id", UInt(64)),
    Column("id", UInt(64)),
    Column("status", UInt(8, Modifiers(nullable=True))),
    Column("last_seen", DateTime(Modifiers(nullable=True))),
    Column("first_seen", DateTime(Modifiers(nullable=True))),
    Column("active_at", DateTime(Modifiers(nullable=True))),
    Column("first_release_id", UInt(64, Modifiers(nullable=True))),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS,
                table_name="groupedmessage_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.EVENTS,
                    version_column="offset",
                    order_by="(project_id, id)",
                    sample_by="id",
                    unsharded=True,
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="groupedmessage_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS,
                table_name="groupedmessage_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="groupedmessage_local", sharding_key=None,
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="groupedmessage_dist",
            )
        ]
