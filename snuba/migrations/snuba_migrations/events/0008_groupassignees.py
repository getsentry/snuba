from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime, UInt, nullable
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines

columns = [
    # Kafka topic offset
    Column("offset", UInt(64)),
    Column("record_deleted", UInt(8)),
    # PG columns
    Column("project_id", UInt(64)),
    Column("group_id", UInt(64)),
    Column("date_added", DateTime(nullable())),
    Column("user_id", UInt(64, nullable())),
    Column("team_id", UInt(64, nullable())),
]


class Migration(migration.MultiStepMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS,
                table_name="groupassignee_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.EVENTS,
                    version_column="offset",
                    order_by="(project_id, group_id)",
                    unsharded=True,
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="groupassignee_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS,
                table_name="groupassignee_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="groupassignee_local", sharding_key=None,
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="groupassignee_dist",
            )
        ]
