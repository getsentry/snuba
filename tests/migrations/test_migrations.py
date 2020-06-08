from snuba.migrations import migrations


def test_run_migration() -> None:
    manager = migrations.Runner()
    manager.run_migration(migrations.App.SYSTEM, "0001_migrations")

    from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
    from snuba.clusters.storage_sets import StorageSetKey

    connection = get_cluster(StorageSetKey.MIGRATIONS).get_query_connection(
        ClickhouseClientSettings.MIGRATE
    )
    assert connection.execute(
        "SELECT app, migration_id, status FROM migrations_local FINAL;"
    ) == [("system", "0001_migrations", "completed")]
