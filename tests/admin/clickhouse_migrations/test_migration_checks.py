from typing import Optional, Sequence, Tuple
from unittest.mock import Mock, patch

import pytest

from snuba.admin.clickhouse.migration_checks import (
    ReverseReason,
    ReverseResult,
    RunReason,
    RunResult,
    StatusChecker,
)
from snuba.migrations.group_loader import DirectoryLoader, GroupLoader
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.runner import MigrationDetails, MigrationKey, Runner
from snuba.migrations.status import Status


class ExampleLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("")

    def get_migrations(self) -> Sequence[str]:
        return ["0001", "0002", "0003"]


def group_loader() -> GroupLoader:
    return ExampleLoader()


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


@pytest.mark.clickhouse_db
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


from snuba.admin.clickhouse.migration_checks import run_migration_checks_and_policies


@patch(
    "snuba.admin.clickhouse.migration_checks.get_group_loader",
    return_value=group_loader(),
)
@patch("snuba.admin.clickhouse.migration_checks.StatusChecker")
@patch("snuba.migrations.runner.Runner")
@pytest.mark.parametrize(
    "policy_result, status_result, expected",
    [
        # Policy          Status          Final Result
        # [Run, Reverse], [Run, Reverse], [Can Run, Can Reverse]
        ((True, True), (True, True), (True, True)),
        ((True, False), (True, True), (True, False)),
        ((False, True), (True, True), (False, True)),
        ((False, True), (True, False), (False, False)),
    ],
)
def test_run_migration_checks_and_policies(
    group_loader: Mock,
    mock_checker: Mock,
    mock_runner: Mock,
    policy_result: Tuple[bool, bool],
    status_result: Tuple[bool, bool],
    expected: Tuple[bool, bool],
) -> None:
    mock_policy = Mock()
    checker = mock_checker()
    mock_runner.show_all.return_value = [
        (MigrationGroup("events"), [MigrationDetails("0001", Status.COMPLETED, True)])
    ]

    mock_policy.can_run.return_value = policy_result[0]
    mock_policy.can_reverse.return_value = policy_result[1]
    checker.can_run.return_value = (
        RunResult(True) if status_result[0] else RunResult(False, RunReason.ALREADY_RUN)
    )
    checker.can_reverse.return_value = (
        ReverseResult(True)
        if status_result[1]
        else ReverseResult(False, ReverseReason.NOT_RUN_YET)
    )

    checks = run_migration_checks_and_policies({"events": {mock_policy}}, mock_runner)
    _, migration_ids = checks[0]
    assert migration_ids[0].can_run == expected[0]
    assert migration_ids[0].can_reverse == expected[1]
