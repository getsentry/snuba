from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime, Enum, String, UInt
from snuba.clusters.cluster import get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.context import Context
from snuba.migrations.status import Status
from snuba.migrations.table_engines import Distributed, ReplacingMergeTree

columns: Sequence[Column[Modifiers]] = [
    Column("group", String()),
    Column("migration_id", String()),
    Column("timestamp", DateTime()),
    Column("status", Enum([("completed", 0), ("in_progress", 1), ("not_started", 2)]),),
    Column("version", UInt(64, Modifiers(default="1"))),
]


class Migration(migration.Migration):
    """
    This migration extends Migration instead of MultiStepMigration since it is
    responsible for bootstrapping the migration system itself. It skips setting
    the in progress status in the forwards method and the not started status in
    the backwards method. Since the migration table doesn't exist yet, we can't
    write any statuses until this migration is completed.
    """

    blocking = False

    def __forwards_local(self) -> Sequence[operations.SqlOperation]:
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

    def __backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.MIGRATIONS, table_name="migrations_local",
            )
        ]

    def __forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.MIGRATIONS,
                table_name="migrations_dist",
                columns=columns,
                engine=Distributed(
                    local_table_name="migrations_local", sharding_key=None
                ),
            )
        ]

    def __backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.MIGRATIONS, table_name="migrations_dist"
            )
        ]

    def forwards(self, context: Context, dry_run: bool) -> None:
        if dry_run:
            self.__dry_run(self.__forwards_local(), self.__forwards_dist())
            return

        migration_id, logger, update_status = context
        logger.info(f"Running migration: {migration_id}")
        for op in self.__forwards_local():
            op.execute(local=True)
        for op in self.__forwards_dist():
            op.execute(local=False)
        logger.info(f"Finished: {migration_id}")
        update_status(Status.COMPLETED)

    def backwards(self, context: Context, dry_run: bool) -> None:
        if dry_run:
            self.__dry_run(self.__backwards_local(), self.__backwards_dist())
            return

        migration_id, logger, update_status = context
        logger.info(f"Reversing migration: {migration_id}")
        update_status(Status.IN_PROGRESS)
        for op in self.__backwards_dist():
            op.execute(local=False)
        for op in self.__backwards_local():
            op.execute(local=True)
        logger.info(f"Finished reversing: {migration_id}")

    def __dry_run(
        self,
        local_operations: Sequence[operations.SqlOperation],
        dist_operations: Sequence[operations.SqlOperation],
    ) -> None:

        print("Local operations:")

        for op in local_operations:
            print(op.format_sql())

        print("\n")
        print("Dist operations:")

        for op in dist_operations:
            cluster = get_cluster(op._storage_set)

            if not cluster.is_single_node():
                print(op.format_sql())
            else:
                print("Skipped dist operation - single node cluster")
