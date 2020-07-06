from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.groups import get_group_loader, MigrationGroup
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
    assert len(runner._get_pending_migrations()[1]) == total_migrations - 1


def test_run_all() -> None:
    runner = Runner()
    assert len(runner._get_pending_migrations()[1]) == get_total_migration_count()

    runner.run_all()
    assert runner._get_pending_migrations()[1] == []


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
