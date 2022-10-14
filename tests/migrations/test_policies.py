from unittest.mock import patch

import pytest

from snuba.migrations.groups import MigrationGroup
from snuba.migrations.policies import (
    ReadOnlyPolicy,
    WriteAllPolicy,
    WriteSafeAndPendingPolicy,
)
from snuba.migrations.runner import MigrationKey
from snuba.migrations.status import Status

POLICIES = {
    "read_only": ReadOnlyPolicy(),
    "write_pending": WriteSafeAndPendingPolicy(),
    "write_all": WriteAllPolicy(),
}


def code_migration_key() -> None:
    """
    Code Migration with blocking == True
    """
    return MigrationKey(MigrationGroup("events"), "0014_backfill_errors")


def sql_migration_key() -> None:
    """
    SQL Migration with blocking == False
    """
    return MigrationKey(MigrationGroup("events"), "0015_truncate_events")


class TestMigrationPolicies:
    @pytest.mark.parametrize(
        "migration_key, action, policy, expected",
        [
            pytest.param(
                code_migration_key(),
                "run",
                "read_only",
                False,
                id="ReadOnly Code Migration",
            ),
            pytest.param(
                sql_migration_key(),
                "run",
                "read_only",
                False,
                id="ReadOnly SQL Migration",
            ),
            pytest.param(
                code_migration_key(),
                "run",
                "write_pending",
                False,
                id="WriteSafeAndPending Code Migration",
            ),
            pytest.param(
                sql_migration_key(),
                "run",
                "write_pending",
                True,
                id="WriteSafeAndPending SQL Migration",
            ),
            pytest.param(
                code_migration_key(),
                "run",
                "write_all",
                True,
                id="WriteAll Code Migration",
            ),
            pytest.param(
                sql_migration_key(),
                "reverse",
                "write_all",
                True,
                id="WriteAll SQL Migration",
            ),
        ],
    )
    def test_policies(self, migration_key, action, policy, expected) -> None:
        if action == "run":
            assert POLICIES[policy].can_run(migration_key) == expected
        else:
            assert POLICIES[policy].can_reverse(migration_key) == expected

    @patch(
        "snuba.migrations.runner.Runner.get_status",
        return_value=(Status.IN_PROGRESS, None),
    )
    def test_pending_migration_reverse(self, mock_get_status):
        migration_key = MigrationKey(
            MigrationGroup("events"), "0016_drop_legacy_events"
        )
        assert WriteSafeAndPendingPolicy().can_reverse(migration_key) == True
