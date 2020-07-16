from typing import Sequence

from snuba.clickhouse.columns import Column, DateTime, Enum, String, UInt, WithDefault
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.context import Context
from snuba.migrations.status import Status
from snuba.migrations.table_engines import Distributed, ReplacingMergeTree


columns = [
    Column("group", String()),
    Column("migration_id", String()),
    Column("timestamp", DateTime()),
    Column("status", Enum([("completed", 0), ("in_progress", 1), ("not_started", 2)]),),
    Column("version", WithDefault(UInt(64), "1")),
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

    def __forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.MIGRATIONS,
                table_name="migrations_local",
                columns=columns,
                engine=ReplacingMergeTree(
                    version_column="version", order_by="(group, migration_id)"
                ),
            ),
        ]

    def __backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.MIGRATIONS, table_name="migrations_local",
            )
        ]

    def __forwards_dist(self) -> Sequence[operations.Operation]:
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

    def __backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.MIGRATIONS, table_name="migrations_dist"
            )
        ]

    def forwards(self, context: Context) -> None:
        migration_id, logger, update_status = context
        logger.info(f"Running migration: {migration_id}")
        for op in self.__forwards_local():
            op.execute()
        # TODO: Run the forwards_dist operations here when multi node clusters are supported
        logger.info(f"Finished: {migration_id}")
        update_status(Status.COMPLETED)

    def backwards(self, context: Context) -> None:
        migration_id, logger, update_status = context
        logger.info(f"Reversing migration: {migration_id}")
        for op in self.__backwards_local():
            op.execute()
        # TODO: Run the backwards_dist operations here when multi node clusters are supported
        logger.info(f"Finished reversing: {migration_id}")
        update_status(Status.NOT_STARTED)
