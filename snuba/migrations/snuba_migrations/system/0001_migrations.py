from typing import Sequence
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration
from snuba.migrations import operations


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
