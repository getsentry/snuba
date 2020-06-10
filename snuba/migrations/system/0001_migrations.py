from typing import Sequence
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration
from snuba.migrations import operations


class Migration(migration.Migration):
    is_dangerous = False
    dependency = None

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.RunSql(
                storage_set=StorageSetKey.MIGRATIONS,
                statement="""\
                    CREATE TABLE migrations_local (app String, migration_id String,
                    timestamp DateTime, status Enum('completed' = 0, 'in_progress' = 1))
                    ENGINE = ReplacingMergeTree(timestamp) ORDER BY (app, migration_id);
                """,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.MIGRATIONS, table_name="migrations_local",
            )
        ]
