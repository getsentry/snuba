from abc import ABC, abstractmethod

from snuba.migrations.groups import get_group_loader
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status


class MigrationPolicy(ABC):
    """
    A MigrationPolicy implements an `is_allowed` method
    that determines whether or not a migration can be applied
    from snuba admin.

    A policy can be used by a migration group to determine the
    level of access a group has to run migrations.
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
    Safe migrations can be run in addition to dangerous migrations that
    are in a pending state. All backwards migrations are considered
    dangerous in this policy.

    This policy defers to the `blocking` attribute for a migration to
    determine if the migration is safe or not.
    """

    def _get_status(self, migration_key: MigrationKey) -> Status:
        status, _ = Runner().get_status(migration_key)
        return status

    def can_run(self, migration_key: MigrationKey) -> bool:
        migration = get_group_loader(migration_key.group).load_migration(
            migration_key.migration_id
        )
        return False if migration.blocking else True

    def can_reverse(self, migration_key: MigrationKey) -> bool:
        if self._get_status(migration_key) == Status.IN_PROGRESS:
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
