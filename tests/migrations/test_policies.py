from datetime import datetime, timedelta
from typing import Mapping
from unittest.mock import Mock, patch

import pytest

from snuba.migrations.groups import MigrationGroup
from snuba.migrations.policies import (
    MAX_REVERT_TIME_WINDOW_HRS,
    AllMigrationsPolicy,
    MigrationPolicy,
    NoMigrationsPolicy,
    NonBlockingMigrationsPolicy,
)
from snuba.migrations.runner import MigrationKey
from snuba.migrations.status import Status

POLICIES: Mapping[str, MigrationPolicy] = {
    "no_migrations": NoMigrationsPolicy(),
    "non_blocking_migrations": NonBlockingMigrationsPolicy(),
    "all_migrations": AllMigrationsPolicy(),
}


def code_migration_key() -> MigrationKey:
    """
    Code Migration with blocking == True
    """
    return MigrationKey(MigrationGroup("events"), "0014_backfill_errors")


def sql_migration_key() -> MigrationKey:
    """
    SQL Migration with blocking == False
    """
    return MigrationKey(MigrationGroup("events"), "0015_truncate_events")


class TestMigrationPolicies:
    @pytest.mark.parametrize(
        "migration_key, action, policy_str, expected",
        [
            pytest.param(
                code_migration_key(),
                "run",
                "no_migrations",
                False,
                id="NoMigrationsPolicy Code Migration",
            ),
            pytest.param(
                sql_migration_key(),
                "run",
                "no_migrations",
                False,
                id="NoMigrationsPolicy SQL Migration",
            ),
            pytest.param(
                code_migration_key(),
                "run",
                "non_blocking_migrations",
                False,
                id="NonBlockingMigrations Code Migration",
            ),
            pytest.param(
                sql_migration_key(),
                "run",
                "non_blocking_migrations",
                True,
                id="NonBlockingMigrations SQL Migration",
            ),
            pytest.param(
                code_migration_key(),
                "run",
                "all_migrations",
                True,
                id="AllMigrations Code Migration",
            ),
            pytest.param(
                sql_migration_key(),
                "reverse",
                "all_migrations",
                True,
                id="AllMigrations SQL Migration",
            ),
        ],
    )
    def test_policies(
        self, migration_key: MigrationKey, action: str, policy_str: str, expected: bool
    ) -> None:
        if action == "run":
            assert POLICIES[policy_str].can_run(migration_key) == expected
        else:
            assert POLICIES[policy_str].can_reverse(migration_key) == expected

    @patch(
        "snuba.migrations.runner.Runner.get_status",
        return_value=(Status.IN_PROGRESS, None),
    )
    def test_pending_migration_reverse(self, mock_get_status: Mock) -> None:
        migration_key = MigrationKey(
            MigrationGroup("events"), "0016_drop_legacy_events"
        )
        assert NonBlockingMigrationsPolicy().can_reverse(migration_key) == True

        # if forward migration was blocking, reverse not allowed even when pending
        migration_key = code_migration_key()
        assert NonBlockingMigrationsPolicy().can_reverse(migration_key) == False

    @patch(
        "snuba.migrations.runner.Runner.get_status",
        return_value=(
            Status.COMPLETED,
            datetime.now() + timedelta(hours=-(MAX_REVERT_TIME_WINDOW_HRS - 5)),
        ),
    )
    def test_completed_migration_reverse(self, mock_get_status: Mock) -> None:
        migration_key = MigrationKey(
            MigrationGroup("events"), "0016_drop_legacy_events"
        )
        assert NonBlockingMigrationsPolicy().can_reverse(migration_key) == True

        # if forward migration was blocking, reverse not allowed even when
        # within the max time limit to
        migration_key = code_migration_key()
        assert NonBlockingMigrationsPolicy().can_reverse(migration_key) == False
