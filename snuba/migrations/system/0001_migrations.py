from typing import Sequence
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migrations
from snuba.migrations import operations


class Migration(migrations.Migration):
    is_dangerous = False
    dependencies = None

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.RunSql(
                storage_set=StorageSetKey.MIGRATIONS,
                statement="""\
                    CREATE TABLE IF NOT EXISTS migrations_local (app String,
                    migration_id String, timestamp DateTime, status Enum('completed' = 0,
                    'in_progress' = 1), consumer_offsets String)
                    ENGINE = ReplacingMergeTree() ORDER BY (app, migration_id);
                """,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.MIGRATIONS, table_name="migrations_local",
            )
        ]
