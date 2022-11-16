from typing import Optional, Sequence, Union
from unittest.mock import Mock, patch

import pytest

from snuba.admin.clickhouse.migration_checks import (
    PolicyChecker,
    Result,
    ReverseReason,
    ReverseResult,
    RunReason,
    RunResult,
    StatusChecker,
    do_checks,
)
from snuba.migrations.groups import DirectoryLoader, GroupLoader, MigrationGroup
from snuba.migrations.runner import MigrationDetails, MigrationKey, Runner
from snuba.migrations.status import Status


class ExampleLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("")

    def get_migrations(self) -> Sequence[str]:
        return ["0001", "0002", "0003"]


def group_loader() -> GroupLoader:
    return ExampleLoader()


def test_do_checks() -> None:
    checker1 = Mock()
    checker2 = Mock()

    checker1.can_run.return_value = RunResult(False, RunReason.ALREADY_RUN)
    checker1.can_reverse.return_value = ReverseResult(False, ReverseReason.NOT_RUN_YET)
    checker2.can_run.return_value = RunResult(True)
    checker2.can_reverse.return_value = ReverseResult(False, ReverseReason.NOT_RUN_YET)

    run, reverse = do_checks(
        [checker1, checker2], MigrationKey(MigrationGroup("querylog"), "0001_xxx")
    )
    assert run.allowed == False
    assert reverse.allowed == False

    assert checker2.can_run.call_count == 0
    assert checker2.can_reverse.call_count == 0

    run, reverse = do_checks(
        [checker2, checker1], MigrationKey(MigrationGroup("querylog"), "0001_xxx")
    )
    assert run.allowed == False
    assert reverse.allowed == False

    assert checker1.can_run.call_count == 2
    assert checker1.can_reverse.call_count == 1


RUN_MIGRATIONS: Sequence[MigrationDetails] = [
    MigrationDetails("0001", Status.COMPLETED, True),
    MigrationDetails("0002", Status.NOT_STARTED, True),
    MigrationDetails("0003", Status.NOT_STARTED, True),
]


@patch(
    "snuba.admin.clickhouse.migration_checks.get_group_loader",
    return_value=group_loader(),
)
@pytest.mark.parametrize(
    "migration_id, expected_allowed, expected_reason",
    [
        pytest.param("0001", False, RunReason.ALREADY_RUN),
        pytest.param("0002", True, None),
        pytest.param("0003", False, RunReason.NEEDS_EARLIER_MIGRATIONS),
    ],
)
def test_status_checker_run(
    mock_loader: Mock,
    migration_id: str,
    expected_allowed: bool,
    expected_reason: Optional[RunReason],
) -> None:
    group = MigrationGroup("querylog")
    checker = StatusChecker(group, RUN_MIGRATIONS)
    result = checker.can_run(MigrationKey(group, migration_id))

    assert result.allowed == expected_allowed
    assert result.reason == expected_reason


REVERSE_MIGRATIONS: Sequence[MigrationDetails] = [
    MigrationDetails("0001", Status.COMPLETED, True),
    MigrationDetails("0002", Status.IN_PROGRESS, True),
    MigrationDetails("0003", Status.NOT_STARTED, True),
]


@patch(
    "snuba.admin.clickhouse.migration_checks.get_group_loader",
    return_value=group_loader(),
)
@pytest.mark.parametrize(
    "migration_id, expected_allowed, expected_reason",
    [
        pytest.param("0001", False, ReverseReason.NEEDS_SUBSEQUENT_MIGRATIONS),
        pytest.param("0002", True, None),
        pytest.param("0003", False, ReverseReason.NOT_RUN_YET),
    ],
)
def test_status_checker_reverse(
    mock_loader: Mock,
    migration_id: str,
    expected_allowed: bool,
    expected_reason: Optional[ReverseReason],
) -> None:
    group = MigrationGroup("querylog")
    checker = StatusChecker(group, REVERSE_MIGRATIONS)
    result = checker.can_reverse(MigrationKey(group, migration_id))

    assert result.allowed == expected_allowed
    assert result.reason == expected_reason


def test_status_checker_errors() -> None:
    """
    Tests the following failure cases:
    * status checker initalized with mismatching group and migrations

    * status checker called with MigrationKey whose migration doesn't
      match the migrations the status checker is initalized with

    * status checker called with MigrationKey whose group doesn't match
      the group the status checker is initialized with
    """
    group = MigrationGroup("querylog")
    with pytest.raises(AssertionError, match="migration ids dont match"):
        StatusChecker(group, RUN_MIGRATIONS)

    runner = Runner()
    migration_id = "0666_wrong_migration"
    _, migrations = runner.show_all(["querylog"])[0]
    checker = StatusChecker(group, migrations)

    with pytest.raises(
        AssertionError, match="0666_wrong_migration is not part of querylog group"
    ):
        checker.can_run(MigrationKey(group, migration_id))

    with pytest.raises(AssertionError, match="Group events does not match querylog"):
        checker.can_reverse(MigrationKey(MigrationGroup("events"), migration_id))


@pytest.mark.parametrize(
    "migration_id, policy, action, expected_allowed, expected_reason",
    [
        pytest.param(
            "0001_querylog",
            "AllMigrationsPolicy",
            "run",
            True,
            None,
        ),
        pytest.param(
            "0001_querylog",
            "NoMigrationsPolicy",
            "reverse",
            False,
            ReverseReason.REVERSE_POLICY,
        ),
    ],
)
def test_policy_checker_run(
    migration_id: str,
    policy: str,
    action: str,
    expected_allowed: bool,
    expected_reason: Optional[Union[RunReason, ReverseReason]],
) -> None:

    with patch(
        "snuba.settings.ADMIN_ALLOWED_MIGRATION_GROUPS",
        {"querylog": policy},
    ):
        group = MigrationGroup("querylog")
        checker = PolicyChecker()
        if action == "run":
            result: Result = checker.can_run(MigrationKey(group, migration_id))
        if action == "reverse":
            result = checker.can_reverse(MigrationKey(group, migration_id))

        assert result.allowed == expected_allowed
        assert result.reason == expected_reason
