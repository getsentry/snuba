from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.migrations.groups import get_group_loader, MigrationGroup
from snuba.migrations.parse_schema import get_local_schema
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status


def setup_function() -> None:
    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    connection.execute("DROP TABLE IF EXISTS migrations_local;")


def test_run_migration() -> None:
    runner = Runner()
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))

    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    assert connection.execute(
        "SELECT group, migration_id, status, version FROM migrations_local;"
    ) == [("system", "0001_migrations", "completed", 1)]


def test_get_pending_migrations() -> None:
    runner = Runner()
    total_migrations = get_total_migration_count()
    assert len(runner._get_pending_migrations()) == total_migrations
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    assert len(runner._get_pending_migrations()) == total_migrations - 1


def test_run_all() -> None:
    runner = Runner()
    assert len(runner._get_pending_migrations()) == get_total_migration_count()

    runner.run_all()
    assert runner._get_pending_migrations() == []


def get_total_migration_count() -> int:
    count = 0
    for group in MigrationGroup:
        count += len(get_group_loader(group).get_migrations())
    return count


def test_version() -> None:
    runner = Runner()
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    migration_key = MigrationKey(MigrationGroup.EVENTS, "test")
    assert runner._get_next_version(migration_key) == 1
    runner._update_migration_status(migration_key, Status.IN_PROGRESS)
    assert runner._get_next_version(migration_key) == 2
    runner._update_migration_status(migration_key, Status.COMPLETED)
    assert runner._get_next_version(migration_key) == 3


def test_no_schema_differences() -> None:
    storages_to_test = [
        StorageKey.TRANSACTIONS,
        StorageKey.QUERYLOG,
    ]  # TODO: Eventually test all storages

    runner = Runner()
    runner.run_all()

    for storage_key in storages_to_test:
        storage = get_storage(storage_key)
        conn = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )

        for schema in storage.get_schemas().get_unique_schemas():
            if not isinstance(schema, TableSchema):
                continue

            table_name = schema.get_local_table_name()
            local_schema = get_local_schema(conn, table_name)

            assert (
                schema.get_column_differences(local_schema) == []
            ), f"Schema mismatch: {table_name} does not match schema"
