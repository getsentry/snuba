from typing import Sequence
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration
from snuba.migrations import operations
from snuba.migrations.context import Context
from snuba.migrations.status import Status


class Migration(migration.Migration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
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

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.MIGRATIONS, table_name="migrations_local",
            )
        ]

    def forwards(self, context: Context) -> None:
        migration_id, logger, update_status = context
        logger.info(f"Running migration: {migration_id}")
        for op in self.forwards_local():
            op.execute()
        logger.info(f"Finished: {migration_id}")
        update_status(Status.COMPLETED)
