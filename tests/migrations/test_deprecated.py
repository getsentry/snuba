import importlib
from typing import Any, Sequence

from unittest.mock import patch

from snuba.migrations import context, groups, migration, runner
from snuba.migrations.status import Status


def teardown_function() -> None:
    importlib.reload(runner)


class DeprecatedMigration(migration.Migration):
    blocking = False
    deprecated = True

    def forwards(self, context: context.Context) -> None:
        context.update_status(Status.COMPLETED)

    def backwards(self, context: context.Context) -> None:
        pass


class FakeEventsLoader(groups.GroupLoader):
    def get_migrations(self) -> Sequence[str]:
        return ["test"]

    def load_migration(self, migration_id: str) -> migration.Migration:
        return DeprecatedMigration()


@patch("snuba.migrations.groups.get_group_loader")
def test_deprecated_migrations(get_group_loader: Any) -> None:
    importlib.reload(runner)

    get_group_loader.side_effect = (
        lambda group: FakeEventsLoader()
        if group == groups.MigrationGroup.EVENTS
        else groups._REGISTERED_GROUPS[group]
    )
    migration_runner = runner.Runner()
    migration_runner.run_migration(
        runner.MigrationKey(groups.MigrationGroup.SYSTEM, "0001_migrations")
    )
    migration_runner.run_migration(
        runner.MigrationKey(groups.MigrationGroup.EVENTS, "test")
    )

    assert migration_runner._get_pending_migrations()[0] == [
        runner.MigrationKey(groups.MigrationGroup.EVENTS, "test")
    ]
