from typing import Sequence
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration
from snuba.migrations import operations
from snuba.migrations.context import MigrationContext
from snuba.migrations.status import Status


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
            operations.RunSql(
                storage_set=StorageSetKey.MIGRATIONS,
                statement="""\
                    CREATE TABLE migrations_local (group String, migration_id String,
                    timestamp DateTime, status Enum('completed' = 0, 'in_progress' = 1,
                    'not_started' = 2), version UInt64 DEFAULT 1)
                    ENGINE = ReplacingMergeTree(version) ORDER BY (group, migration_id);
                """,
            ),
        ]

    def __backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.MIGRATIONS, table_name="migrations_local",
            )
        ]

    def forwards(self, context: MigrationContext) -> None:
        migration_id, logger, update_status = context
        logger.info(f"Running migration: {migration_id}")
        for op in self.__forwards_local():
            op.execute()
        logger.info(f"Finished: {migration_id}")
        update_status(Status.COMPLETED)
