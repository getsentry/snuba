from typing import Any, Sequence

from unittest.mock import patch

from snuba.migrations.groups import GroupLoader
from snuba.migrations import context, migration
from snuba.migrations.status import Status


class DeprecatedMigration(migration.Migration):
    blocking = False
    deprecated = True

    def forwards(self, context: context.Context) -> None:
        context.update_status(Status.COMPLETED)

    def backwards(self, context: context.Context) -> None:
        pass


class FakeEventsLoader(GroupLoader):
    def get_migrations(self) -> Sequence[str]:
        return ["test"]

    def load_migration(self, migration_id: str) -> migration.Migration:
        return DeprecatedMigration()


@patch("snuba.migrations.groups.get_group_loader")
def test_deprecated_migrations(get_group_loader: Any) -> None:
    from snuba.migrations.groups import MigrationGroup, _REGISTERED_GROUPS
    from snuba.migrations.runner import MigrationKey, Runner

    get_group_loader.side_effect = (
        lambda group: FakeEventsLoader()
        if group == MigrationGroup.EVENTS
        else _REGISTERED_GROUPS[group]
    )
    runner = Runner()
    runner.run_migration(MigrationKey(MigrationGroup.SYSTEM, "0001_migrations"))
    runner.run_migration(MigrationKey(MigrationGroup.EVENTS, "test"))

    assert runner._get_pending_migrations()[0] == [
        MigrationKey(MigrationGroup.EVENTS, "test")
    ]
