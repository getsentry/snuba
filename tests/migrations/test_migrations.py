from snuba.migrations import migrations


def test_run_migration() -> None:
    manager = migrations.Runner()
    manager.run_migration(migrations.App.SYSTEM, "0001_migrations")
