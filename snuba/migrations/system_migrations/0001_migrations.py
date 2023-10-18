from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime, Enum, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.table_engines import Distributed, ReplacingMergeTree

columns: Sequence[Column[Modifiers]] = [
    Column("group", String()),
    Column("migration_id", String()),
    Column("timestamp", DateTime()),
    Column(
        "status",
        Enum([("completed", 0), ("in_progress", 1), ("not_started", 2)]),
    ),
    Column("version", UInt(64, Modifiers(default="1"))),
]


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    This migration is the only one that sets is_first_migration = True since it is
    responsible for bootstrapping the migration system itself. It skips setting
    the in progress status in the forwards method and the not started status in
    the backwards method. Since the migration table doesn't exist yet, we can't
    write any statuses until this migration is completed.
    """

    blocking = True

    def is_first_migration(self) -> bool:
        return True

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.MIGRATIONS,
                table_name="migrations_local",
                columns=columns,
                engine=ReplacingMergeTree(
                    storage_set=StorageSetKey.MIGRATIONS,
                    version_column="version",
                    order_by="(group, migration_id)",
                ),
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.MIGRATIONS,
                table_name="migrations_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.MIGRATIONS,
                table_name="migrations_dist",
                columns=columns,
                engine=Distributed(
                    local_table_name="migrations_local",
                    sharding_key="cityHash64(group)",
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.MIGRATIONS, table_name="migrations_dist"
            )
        ]
