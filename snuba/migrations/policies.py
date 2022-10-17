from abc import ABC, abstractmethod

from snuba.migrations.groups import get_group_loader
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status


class MigrationPolicy(ABC):
    """
    A MigrationPolicy implements `can_run` and
    `can_reverse` methods that determines whether or not
    a migration can be run or reversed from snuba admin.

    A policy can be used by a migration group to determine the
    level of access a group has to run/reverse migrations.
    """

    @abstractmethod
    def can_run(self, migration_key: MigrationKey) -> bool:
        raise NotImplementedError

    @abstractmethod
    def can_reverse(self, migration_key: MigrationKey) -> bool:
        raise NotImplementedError


class ReadOnlyPolicy(MigrationPolicy):
    """
    No migration is allowed to be run or reversed.
    """

    def can_run(self, migration_key: MigrationKey) -> bool:
        return False

    def can_reverse(self, migration_key: MigrationKey) -> bool:
        return False


class WriteSafeAndPendingPolicy(MigrationPolicy):
    """
    Migrations can be run that are non-blocking, as determined
    by the `blocking` attribute set on a migration.

    Reversing a migration is considered blocking by this policy,
    and only migrations in a pending state can be reversed.
    """

    def can_run(self, migration_key: MigrationKey) -> bool:
        migration = get_group_loader(migration_key.group).load_migration(
            migration_key.migration_id
        )
        return False if migration.blocking else True

    def can_reverse(self, migration_key: MigrationKey) -> bool:
        status, _ = Runner().get_status(migration_key)
        if status == Status.IN_PROGRESS:
            return True
        return False


class WriteAllPolicy(MigrationPolicy):
    """
    All migrations are allowed - both CodeMigrations and
    ClickhouseNodeMigration (SQL) migrations
    """

    def can_run(self, migration_key: MigrationKey) -> bool:
        return True

    def can_reverse(self, migration_key: MigrationKey) -> bool:
        return True
