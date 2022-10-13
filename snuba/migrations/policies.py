from abc import ABC, abstractmethod
from enum import Enum

from snuba.migrations.migration import Migration
from snuba.migrations.status import Status


class MigrationAction(Enum):
    FORWARDS = "forwards"
    BACKWARDS = "backwards"


class MigrationPolicy(ABC):
    """
    A MigrationPolicy implements an `is_allowed` method
    that determines whether or not a migration can be applied
    from snuba admin.

    A policy can be used by a migration group to determine the
    level of access a group has to run migrations.
    """

    @abstractmethod
    def allows(migration: Migration, action: MigrationAction) -> bool:
        raise NotImplementedError


class ReadOnlyPolicy(MigrationPolicy):
    """
    No migration is allowed to be run or reversed.
    """

    def allows(migration: Migration, action: MigrationAction) -> bool:
        return False


class WriteSafeAndPendingPolicy(MigrationPolicy):
    """
    Safe migrations can be run in addition to dangerous migrations that
    are in a pending state. All backwards migrations are considered
    dangerous in this policy.

    This policy defers to the `blocking` attribute for a migration to
    determine if the migration is safe or not.
    """

    def allows(migration: Migration, action: MigrationAction) -> bool:
        if action == MigrationAction.BACKWARDS:
            # todo: actually handle getting the status
            if migration.status == Status.IN_PROGRESS:
                return True
            return False
        return False if migration.blocking else True


class WriteAllPolicy(MigrationPolicy):
    """
    All migrations are allowed - both CodeMigrations and
    ClickhouseNodeMigration (SQL) migrations
    """

    def allows(migration: Migration, action: MigrationAction) -> bool:
        return True
