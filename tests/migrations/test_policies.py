import pytest

from snuba.migrations.groups import MigrationGroup, get_group_loader
from snuba.migrations.policies import (
    MigrationAction,
    ReadOnlyPolicy,
    WriteAllPolicy,
    WriteSafeAndPendingPolicy,
)
from snuba.migrations.status import Status


def code_migration() -> None:
    """
    Code Migration with blocking == True
    """
    events_loader = get_group_loader(MigrationGroup("events"))
    return events_loader.load_migration("0014_backfill_errors")


def sql_migration() -> None:
    """
    SQL Migration with blocking == False
    """
    events_loader = get_group_loader(MigrationGroup("events"))
    return events_loader.load_migration("0015_truncate_events")


def pending_migration() -> None:
    """
    SQL Migration with blocking == False, and with a status set
    """
    events_loader = get_group_loader(MigrationGroup("events"))
    m = events_loader.load_migration("0016_drop_legacy_events")
    m.status = Status.IN_PROGRESS
    return m


class TestMigrationPolicies:
    @pytest.mark.parametrize(
        "migration, action, policy, expected",
        [
            pytest.param(
                code_migration(),
                MigrationAction.FORWARDS,
                ReadOnlyPolicy,
                False,
                id="ReadOnly Code Migration",
            ),
            pytest.param(
                sql_migration(),
                MigrationAction.FORWARDS,
                ReadOnlyPolicy,
                False,
                id="ReadOnly SQL Migration",
            ),
            pytest.param(
                code_migration(),
                MigrationAction.FORWARDS,
                WriteSafeAndPendingPolicy,
                False,
                id="WriteSafeAndPending Code Migration",
            ),
            pytest.param(
                pending_migration(),
                MigrationAction.BACKWARDS,
                WriteSafeAndPendingPolicy,
                True,
                id="WriteSafeAndPending Pending Migration",
            ),
            pytest.param(
                code_migration(),
                MigrationAction.FORWARDS,
                WriteAllPolicy,
                True,
                id="WriteAll Code Migration",
            ),
            pytest.param(
                sql_migration(),
                MigrationAction.BACKWARDS,
                WriteAllPolicy,
                True,
                id="WriteAll SQL Migration",
            ),
        ],
    )
    def test_policies(self, migration, action, policy, expected) -> None:
        assert policy.allows(migration, action) == expected
