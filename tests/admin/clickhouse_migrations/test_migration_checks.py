from typing import Sequence
from unittest.mock import Mock

import pytest

from snuba.admin.clickhouse.migration_checks import (
    Reason,
    ResultReason,
    StatusChecker,
    do_checks,
)
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.runner import MigrationDetails
from snuba.migrations.status import Status

PASS_REASON = ResultReason(True, Reason.NO_REASON_NEEDED)
FAIL_REASON = ResultReason(False, Reason.ALREADY_RUN)


def test_do_checks() -> None:
    checker1 = Mock()
    checker2 = Mock()

    checker1.can_run.return_value = FAIL_REASON
    checker1.can_reverse.return_value = FAIL_REASON
    checker2.can_run.return_value = PASS_REASON
    checker2.can_reverse.return_value = FAIL_REASON

    run, reverse = do_checks([checker1, checker2], "0001_xxx")
    assert run.allowed == False
    assert reverse.allowed == False

    assert checker2.can_run.call_count == 0
    assert checker2.can_reverse.call_count == 0

    run, reverse = do_checks([checker2, checker1], "0001_xxx")
    assert run.allowed == False
    assert reverse.allowed == False

    assert checker1.can_run.call_count == 2
    assert checker1.can_reverse.call_count == 1


RUN_MIGRATIONS: Sequence[MigrationDetails] = [
    MigrationDetails("0001", Status.COMPLETED, True),
    MigrationDetails("0002", Status.NOT_STARTED, True),
    MigrationDetails("0003", Status.NOT_STARTED, True),
]


@pytest.mark.parametrize(
    "migration_id, expected_allowed, expected_reason",
    [
        pytest.param("0001", False, Reason.ALREADY_RUN),
        pytest.param("0002", True, Reason.NO_REASON_NEEDED),
        pytest.param("0003", False, Reason.NEEDS_EARLIER_MIGRATIONS),
    ],
)
def test_status_checker_run(
    migration_id: str, expected_allowed: bool, expected_reason: Reason
) -> None:
    checker = StatusChecker(MigrationGroup("querylog"), RUN_MIGRATIONS)
    result = checker.can_run(migration_id)

    assert result.allowed == expected_allowed
    assert result.reason == expected_reason


REVERSE_MIGRATIONS: Sequence[MigrationDetails] = [
    MigrationDetails("0001", Status.COMPLETED, True),
    MigrationDetails("0002", Status.IN_PROGRESS, True),
    MigrationDetails("0003", Status.NOT_STARTED, True),
]


@pytest.mark.parametrize(
    "migration_id, expected_allowed, expected_reason",
    [
        pytest.param("0001", False, Reason.NEEDS_SUBSEQUENT_MIGRATIONS),
        pytest.param("0002", True, Reason.NO_REASON_NEEDED),
        pytest.param("0003", False, Reason.NOT_RUN_YET),
    ],
)
def test_status_checker_reverse(
    migration_id: str, expected_allowed: bool, expected_reason: Reason
) -> None:
    checker = StatusChecker(MigrationGroup("querylog"), REVERSE_MIGRATIONS)
    result = checker.can_reverse(migration_id)

    assert result.allowed == expected_allowed
    assert result.reason == expected_reason
