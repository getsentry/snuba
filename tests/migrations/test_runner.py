import importlib
from datetime import datetime
from typing import Any, Generator
from unittest.mock import MagicMock, patch

import pytest

import snuba.migrations.connect
from snuba import settings
from snuba.clickhouse.native import ClickhouseResult
from snuba.clusters.cluster import CLUSTERS, ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.readiness_state import ReadinessState
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages import factory
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.migrations.connect import check_for_inactive_replicas
from snuba.migrations.errors import InactiveClickhouseReplica, MigrationError
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


@pytest.fixture(autouse=True)
def setup_teardown(clickhouse_db: None) -> Generator[None, None, None]:
    _drop_all_tables()
    yield
    _drop_all_tables()


@pytest.fixture(scope="function")
def temp_settings() -> Any:
    from snuba import settings

    yield settings
    importlib.reload(settings)


@pytest.mark.clickhouse_db
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


@pytest.mark.clickhouse_db
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


@pytest.mark.clickhouse_db
def test_show_all_for_groups() -> None:
    runner = Runner()
    migration_key = MigrationKey(MigrationGroup("system"), "0001_migrations")
    results = runner.show_all(["system"])

    assert len(results) == 1
    group, migrations = results[0]
    assert group == MigrationGroup("system")
    assert all([migration.status == Status.NOT_STARTED for migration in migrations])

    runner.run_migration(migration_key)
    results = runner.show_all(["system"])

    assert len(results) == 1
    group, migrations = results[0]
    assert group == MigrationGroup("system")
    assert all([migration.status == Status.COMPLETED for migration in migrations])


@pytest.mark.clickhouse_db
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


@pytest.mark.clickhouse_db
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


@pytest.mark.clickhouse_db
def test_get_pending_migrations() -> None:
    runner = Runner()
    total_migrations = get_total_migration_count()
    assert len(runner._get_pending_migrations()) == total_migrations
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    assert len(runner._get_pending_migrations()) == total_migrations - 1


@pytest.mark.clickhouse_db
def test_get_pending_migrations_for_group() -> None:
    runner = Runner()
    group = MigrationGroup.EVENTS
    migration_group_count = len(get_group_loader(group).get_migrations())
    assert len(runner._get_pending_migrations_for_group(group)) == migration_group_count

    runner.run_migration(MigrationKey(MigrationGroup("system"), "0001_migrations"))
    assert len(runner._get_pending_migrations_for_group(MigrationGroup.SYSTEM)) == 0


@pytest.mark.clickhouse_db
def test_run_all_with_group() -> None:
    runner = Runner()
    group = MigrationGroup.EVENTS
    event_migration_count = len(get_group_loader(group).get_migrations())
    system_migration_count = len(
        get_group_loader(MigrationGroup.SYSTEM).get_migrations()
    )

    with pytest.raises(MigrationError):
        runner.run_all(force=False, group=group)

    runner.run_all(force=True, group=group)

    # pending system migrations run with any group
    expected_pending_count = (
        get_total_migration_count() - event_migration_count - system_migration_count
    )
    assert len(runner._get_pending_migrations()) == expected_pending_count


@pytest.mark.clickhouse_db
def test_run_all() -> None:
    runner = Runner()
    assert len(runner._get_pending_migrations()) == get_total_migration_count()

    with pytest.raises(MigrationError):
        runner.run_all(force=False)

    runner.run_all(force=True)
    assert runner._get_pending_migrations() == []


@pytest.mark.clickhouse_db
def test_run_all_using_through() -> None:
    """
    Using "through" allows migrating up to (including)
    a specified migration id (or prefix) for a given group.

    `snuba migrations migrate generic_metrics 0003`

    Prefix must match exactly one migration id to be valid.
    """
    runner = Runner()

    with pytest.raises(MigrationError):
        # using through requires a group
        runner.run_all(force=True, through="0001")

    group = MigrationGroup.GENERIC_METRICS
    all_generic_metrics = len(get_group_loader(group).get_migrations())
    assert (
        len(runner._get_pending_migrations_for_group(group=group))
        == all_generic_metrics
    )

    with pytest.raises(MigrationError):
        # too many migrations id matches
        runner.run_all(force=True, group=group, through="00")

    with pytest.raises(MigrationError):
        # no migration id matches
        runner.run_all(force=True, group=group, through="9999")

    runner.run_all(force=True, group=group, through="0002")
    assert len(runner._get_pending_migrations_for_group(group=group)) == (
        all_generic_metrics - 2
    )

    # Running with --fake
    # (generic_metric_sets_aggregation_mv was added in 0003)
    runner.run_all(force=True, group=group, through="0003", fake=True)
    connection = get_cluster(StorageSetKey.GENERIC_METRICS_SETS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    assert (
        connection.execute(
            "SHOW TABLES LIKE 'generic_metric_sets_aggregation_mv'"
        ).results
        == []
    )


@pytest.mark.clickhouse_db
def test_run_all_using_readiness() -> None:
    """
    Using "readiness_state" filtering groups by readiness state.
    """
    runner = Runner()

    group = MigrationGroup.GENERIC_METRICS
    all_generic_metrics = len(get_group_loader(group).get_migrations())
    assert (
        len(runner._get_pending_migrations_for_group(group=group))
        == all_generic_metrics
    )

    # using different readiness wont change anything
    runner.run_all(force=True, group=group, readiness_states=[ReadinessState.LIMITED])
    assert len(runner._get_pending_migrations_for_group(group=group)) == (
        all_generic_metrics
    )

    # using correct readiness state runs the migration
    runner.run_all(force=True, group=group, readiness_states=[ReadinessState.COMPLETE])
    assert len(runner._get_pending_migrations_for_group(group=group)) == 0


@pytest.mark.clickhouse_db
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


@pytest.mark.clickhouse_db
def test_reverse_idempotency_all() -> None:
    # This test is to ensure that reversing a migration twice does not cause any
    # issues or unintended side effects. This is important because we may need to reverse
    # a migration multiple times in the event of a rollback.
    runner = Runner()
    all_migrations = runner._get_pending_migrations()
    runner.run_all(force=True)
    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    for migration in reversed(all_migrations):
        runner.reverse_migration(migration, force=True)
        if migration.group != MigrationGroup.SYSTEM:

            def reverse_twice() -> None:
                # reverse again to ensure idempotency
                runner.run_migration(migration, fake=True)
                runner.reverse_migration(migration, force=True)
                runner.run_migration(migration, fake=True)
                runner.reverse_migration(migration, force=True)

            group = migration.group.value

            # for some reason this has to be here. it looks like
            # there is another test modifying the Exception so it
            # wont be caught if we import too early
            from snuba.clusters.cluster import UndefinedClickhouseCluster

            try:
                cluster_connection = get_cluster(
                    StorageSetKey(group)
                ).get_query_connection(ClickhouseClientSettings.MIGRATE)

                before_state = cluster_connection.execute(
                    "SELECT create_table_query FROM system.tables"
                ).results
                reverse_twice()
                after_state = cluster_connection.execute(
                    "SELECT create_table_query FROM system.tables"
                ).results
                assert before_state == after_state
            except UndefinedClickhouseCluster:
                # Some groups do not have a cluster defined (e.g. test_migration)
                reverse_twice()

    assert (
        connection.execute("SHOW TABLES").results == []
    ), "All tables should be deleted"


@pytest.mark.clickhouse_db
def get_total_migration_count() -> int:
    count = 0
    for group in get_active_migration_groups():
        count += len(get_group_loader(group).get_migrations())
    return count


@pytest.mark.clickhouse_db
def test_get_active_migration_groups(temp_settings: Any) -> None:
    temp_settings.SKIPPED_MIGRATION_GROUPS = {"search_issues"}
    active_groups = get_active_migration_groups()
    assert (
        MigrationGroup.SEARCH_ISSUES not in active_groups
    )  # should be skipped by SKIPPED_MIGRATION_GROUPS

    temp_settings.READINESS_STATE_MIGRATION_GROUPS_ENABLED = {"search_issues"}
    active_groups = get_active_migration_groups()
    assert (
        MigrationGroup.SEARCH_ISSUES in active_groups
    )  # should be active by readiness_state


@pytest.mark.clickhouse_db
def test_reverse_in_progress() -> None:
    runner = Runner()
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    migration_key = MigrationKey(
        MigrationGroup.TEST_MIGRATION, "0001_create_test_table"
    )
    runner.run_migration(migration_key)
    runner._update_migration_status(migration_key, Status.IN_PROGRESS)

    # reversing in progress migrations for system group shouldn't affect
    # the test_migration group
    runner.reverse_in_progress(group=MigrationGroup.SYSTEM)
    assert runner.get_status(migration_key)[0] == Status.IN_PROGRESS

    # reversing in progress with dry run shouldn't execute the reverse sql
    runner.reverse_in_progress(group=MigrationGroup.TEST_MIGRATION, dry_run=True)
    assert runner.get_status(migration_key)[0] == Status.IN_PROGRESS

    # reversing in progress with no groups specified should reverse any in progress
    # migration for any migration group
    runner.reverse_in_progress()
    assert runner.get_status(migration_key)[0] == Status.NOT_STARTED


@pytest.mark.clickhouse_db
def test_version() -> None:
    runner = Runner()
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    migration_key = MigrationKey(MigrationGroup.EVENTS, "test")
    assert runner._get_next_version(migration_key) == 1
    runner._update_migration_status(migration_key, Status.IN_PROGRESS)
    assert runner._get_next_version(migration_key) == 2
    runner._update_migration_status(migration_key, Status.COMPLETED)
    assert runner._get_next_version(migration_key) == 3


@pytest.mark.clickhouse_db
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


@pytest.mark.clickhouse_db
def test_settings_skipped_group() -> None:
    from snuba.migrations import runner

    with patch("snuba.settings.SKIPPED_MIGRATION_GROUPS", {"querylog"}):
        runner.Runner().run_all(force=True)

    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    assert connection.execute("SHOW TABLES LIKE 'querylog_local'").results == []


@pytest.mark.clickhouse_db
def test_check_inactive_replica() -> None:
    inactive_replica_query_result = ClickhouseResult(
        results=[
            ["bad_table_1", 3, 2],
            ["bad_table_2", 4, 1],
        ]
    )

    with patch.object(
        snuba.migrations.connect, "get_all_storage_keys"
    ) as mock_storage_keys:
        storage_key = StorageKey.ERRORS
        storage = get_storage(storage_key)
        database = storage.get_cluster().get_database()
        mock_storage_keys.return_value = [storage_key]

        with patch("snuba.migrations.connect.get_storage") as MockGetStorage:
            mock_cluster = MagicMock()
            MockGetStorage.return_value.get_cluster.return_value = mock_cluster

            mock_conn = mock_cluster.get_node_connection.return_value
            mock_conn.database = database
            mock_conn.execute.return_value = inactive_replica_query_result

            with pytest.raises(InactiveClickhouseReplica) as exc:
                check_for_inactive_replicas()

            assert exc.value.args[0] == (
                f"Storage {storage_key.value} has inactive replicas for table bad_table_1 "
                f"with 2 out of 3 replicas active.\n"
                f"Storage {storage_key.value} has inactive replicas for table bad_table_2 "
                f"with 1 out of 4 replicas active."
            )

            assert mock_conn.execute.call_count == 1
            query = (
                "SELECT table, total_replicas, active_replicas FROM system.replicas "
                f"WHERE active_replicas < total_replicas AND database ='{database}'"
            )
            mock_conn.execute.assert_called_with(query)
