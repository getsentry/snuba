import importlib
from datetime import datetime
from unittest.mock import patch

import pytest

from snuba import settings
from snuba.clusters.cluster import CLUSTERS, ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages import factory
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage
from snuba.migrations.errors import MigrationError
from snuba.migrations.groups import MigrationGroup, get_group_loader
from snuba.migrations.parse_schema import get_local_schema
from snuba.migrations.runner import MigrationKey, Runner, get_active_migration_groups
from snuba.migrations.status import Status


def _drop_all_tables() -> None:
    for cluster in CLUSTERS:
        connection = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
        database = cluster.get_database()

        data = connection.execute(
            f"SELECT name FROM system.tables WHERE database = '{database}'"
        ).results
        for (table,) in data:
            connection.execute(f"DROP TABLE IF EXISTS {table}")


def setup_function() -> None:
    _drop_all_tables()


def teardown_function() -> None:
    _drop_all_tables()


def test_get_status() -> None:
    runner = Runner()
    assert runner.get_status(
        MigrationKey(MigrationGroup.EVENTS, "0001_events_initial")
    ) == (Status.NOT_STARTED, None)
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    assert runner.get_status(
        MigrationKey(MigrationGroup.EVENTS, "0001_events_initial")
    ) == (Status.NOT_STARTED, None)
    runner.run_migration(MigrationKey(MigrationGroup.EVENTS, "0001_events_initial"))
    status = runner.get_status(
        MigrationKey(MigrationGroup.EVENTS, "0001_events_initial")
    )
    assert status[0] == Status.COMPLETED
    assert isinstance(status[1], datetime)


def test_show_all() -> None:
    runner = Runner()
    assert all(
        [
            migration.status == Status.NOT_STARTED
            for (_, group_migrations) in runner.show_all()
            for migration in group_migrations
        ]
    )
    runner.run_all(force=True)
    assert all(
        [
            migration.status == Status.COMPLETED
            for (_, group_migrations) in runner.show_all()
            for migration in group_migrations
        ]
    )


def test_run_migration() -> None:
    runner = Runner()
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))

    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    assert connection.execute(
        "SELECT group, migration_id, status, version FROM migrations_local;"
    ).results == [("system", "0001_migrations", "completed", 1)]

    # Invalid migration ID
    with pytest.raises(MigrationError):
        runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "xxx"))

    # Run out of order
    with pytest.raises(MigrationError):
        runner.run_migration(MigrationKey(MigrationGroup.EVENTS, "0003_errors"))

    # Running with --fake
    runner.run_migration(
        MigrationKey(MigrationGroup.EVENTS, "0001_events_initial"), fake=True
    )
    assert connection.execute("SHOW TABLES LIKE 'sentry_local'").results == []


def test_reverse_migration() -> None:
    runner = Runner()
    runner.run_all(force=True)

    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )

    # Invalid migration ID
    with pytest.raises(MigrationError):
        runner.reverse_migration(MigrationKey(MigrationGroup.SYSTEM, "xxx"))

    with pytest.raises(MigrationError):
        runner.reverse_migration(MigrationKey(MigrationGroup.EVENTS, "0003_errors"))

    # Reverse with --fake
    for migration_id in reversed(
        get_group_loader(MigrationGroup.EVENTS).get_migrations()
    ):
        runner.reverse_migration(
            MigrationKey(MigrationGroup.EVENTS, migration_id), fake=True
        )
    assert (
        len(connection.execute("SHOW TABLES LIKE 'errors_local'").results) == 1
    ), "Table still exists"


def test_get_pending_migrations() -> None:
    runner = Runner()
    total_migrations = get_total_migration_count()
    assert len(runner._get_pending_migrations()) == total_migrations
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    assert len(runner._get_pending_migrations()) == total_migrations - 1


def test_run_all() -> None:
    runner = Runner()
    assert len(runner._get_pending_migrations()) == get_total_migration_count()

    with pytest.raises(MigrationError):
        runner.run_all(force=False)

    runner.run_all(force=True)
    assert runner._get_pending_migrations() == []


def test_reverse_all() -> None:
    runner = Runner()
    all_migrations = runner._get_pending_migrations()
    runner.run_all(force=True)
    for migration in reversed(all_migrations):
        runner.reverse_migration(migration, force=True)

    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    assert (
        connection.execute("SHOW TABLES").results == []
    ), "All tables should be deleted"


def get_total_migration_count() -> int:
    count = 0
    for group in get_active_migration_groups():
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
    settings.ENABLE_DEV_FEATURES = True
    importlib.reload(factory)
    runner = Runner()
    runner.run_all(force=True)

    for storage_key in get_all_storage_keys():
        storage = get_storage(storage_key)
        conn = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.MIGRATE
        )

        schema = storage.get_schema()

        if not isinstance(schema, TableSchema):
            continue

        table_name = schema.get_local_table_name()
        local_schema = get_local_schema(conn, table_name)

        assert (
            schema.get_column_differences(local_schema) == []
        ), f"Schema mismatch: {table_name} does not match schema"

    importlib.reload(settings)
    importlib.reload(factory)


def test_settings_skipped_group() -> None:
    from snuba.migrations import runner

    with patch("snuba.settings.SKIPPED_MIGRATION_GROUPS", {"querylog"}):
        runner.Runner().run_all(force=True)

    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    assert connection.execute("SHOW TABLES LIKE 'querylog_local'").results == []
