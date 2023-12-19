from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from snuba.datasets.readiness_state import ReadinessState
from snuba.migrations.groups import get_group_loader, get_group_readiness_state
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status
from snuba.settings import MAX_MIGRATIONS_REVERT_TIME_WINDOW_HRS
from snuba.utils.registered_class import RegisteredClass


class MigrationPolicy(ABC, metaclass=RegisteredClass):
    """
    A MigrationPolicy implements `can_run` and
    `can_reverse` methods that determines whether or not
    a migration can be run or reversed from snuba admin.

    A policy can be used by a migration group to determine the
    level of access a group has to run/reverse migrations.

    Access to a group is assumed to be done prior to checking
    the policy. A policy doesn't verify access to a group, it
    verifies group access to running/reversing migrations.
    """

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @abstractmethod
    def can_run(self, migration_key: MigrationKey) -> bool:
        raise NotImplementedError

    @abstractmethod
    def can_reverse(self, migration_key: MigrationKey) -> bool:
        raise NotImplementedError


class NoMigrationsPolicy(MigrationPolicy):
    """
    No migration is allowed to be run or reversed.
    """

    def can_run(self, migration_key: MigrationKey) -> bool:
        return False

    def can_reverse(self, migration_key: MigrationKey) -> bool:
        return False


class NonBlockingMigrationsPolicy(MigrationPolicy):
    """
    Migrations can be run that are non-blocking, as determined
    by the `blocking` attribute set on a migration.

    Reversing a migration is considered blocking by this policy,
    and only migrations in a pending state can be reversed. This
    is for the use case where a forward migration was run but
    got stuck in a pending state and needs to be reverted.
    """

    def can_run(self, migration_key: MigrationKey) -> bool:
        if (
            get_group_readiness_state(migration_key.group)
            == ReadinessState.EXPERIMENTAL
        ):
            return True

        migration = get_group_loader(migration_key.group).load_migration(
            migration_key.migration_id
        )
        return False if migration.blocking else True

    def can_reverse(self, migration_key: MigrationKey) -> bool:
        if (
            get_group_readiness_state(migration_key.group)
            == ReadinessState.EXPERIMENTAL
        ):
            return True

        status, timestamp = Runner().get_status(migration_key)
        migration = get_group_loader(migration_key.group).load_migration(
            migration_key.migration_id
        )
        if status == Status.IN_PROGRESS:
            return False if migration.blocking else True

        if status == Status.COMPLETED and timestamp:
            oldest_allowed_timestamp = datetime.now() + timedelta(
                hours=-MAX_MIGRATIONS_REVERT_TIME_WINDOW_HRS
            )
            if timestamp >= oldest_allowed_timestamp:
                return False if migration.blocking else True

        return False


class AllMigrationsPolicy(MigrationPolicy):
    """
    All migrations are allowed - both CodeMigrations and
    ClickhouseNodeMigration (SQL) migrations
    """

    def can_run(self, migration_key: MigrationKey) -> bool:
        return True

    def can_reverse(self, migration_key: MigrationKey) -> bool:
        return True
