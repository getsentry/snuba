from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Sequence, Tuple, Union

from snuba.admin.migrations_policies import get_migration_group_polices
from snuba.migrations.groups import MigrationGroup, get_group_loader
from snuba.migrations.policies import MigrationPolicy
from snuba.migrations.runner import MigrationDetails, MigrationKey
from snuba.migrations.status import Status


class InvalidResultReason(Exception):
    pass


class RunReason(Enum):
    ALREADY_RUN = "already run"
    NEEDS_EARLIER_MIGRATIONS = "earlier migrations must run first"
    RUN_POLICY = "group not allowed run policy"


class ReverseReason(Enum):
    NOT_RUN_YET = "can't reverse if not already run"
    NEEDS_SUBSEQUENT_MIGRATIONS = "subsequent migrations must be reversed first"
    REVERSE_POLICY = "group not allowed reverse policy"


@dataclass
class Result:
    allowed: bool
    reason: Optional[Union[RunReason, ReverseReason]] = None

    def __post_init__(self) -> None:
        if self.allowed and self.reason:
            raise InvalidResultReason("Cannot have 'reason' if 'allowed' is True")
        if not self.allowed and not self.reason:
            raise InvalidResultReason("Must have 'reason' if 'allowed' is False")


@dataclass
class RunResult(Result):
    reason: Optional[RunReason] = None


@dataclass
class ReverseResult(Result):
    reason: Optional[ReverseReason] = None


@dataclass
class MigrationData:
    migration_id: str
    status: str
    blocking: bool
    can_run: bool
    can_reverse: bool
    run_reason: str
    reverse_reason: str


class Checker(ABC):
    """
    A checker is used to encapsulate logic about whether
    a migration can be run or reversed based on some criteria
    or state the checker has.

    If a migration can be run/reversed, the ResultReason that
    is returned should have allowed = True.
    """

    @abstractmethod
    def can_run(self, migration_id: str) -> RunResult:
        raise NotImplementedError

    @abstractmethod
    def can_reverse(self, migration_id: str) -> ReverseResult:
        raise NotImplementedError


class StatusChecker(Checker):
    """
    The StatusChecker validates whether you can run or
    reverse a migration based on the statuses of the
    migration itself and the ones preceeding/following
    it depending on the check.

    Source of truth of migration order is retrieved from
    calling get_migrations() on the group loader, where
    the order is explicitly defined.
    """

    def __init__(
        self,
        migration_group: MigrationGroup,
        migrations: Sequence[MigrationDetails],
    ) -> None:
        self.__group = migration_group
        self.__all_migration_ids: Sequence[str] = get_group_loader(
            migration_group
        ).get_migrations()

        migration_statuses = {}
        for migration_id, status, _ in migrations:
            migration_statuses[migration_id] = {
                "migration_id": migration_id,
                "status": status,
            }
        self.__migration_statuses = migration_statuses

    def can_run(self, migration_id: str) -> RunResult:
        """
        Covers the following cases:
        * no running a migration that is already pending or completed
        * no running a migration that has a preceeding migration that is not completed
        """
        all_migration_ids = self.__all_migration_ids
        if self.__migration_statuses[migration_id]["status"] != Status.NOT_STARTED:
            return RunResult(False, RunReason.ALREADY_RUN)

        for m in all_migration_ids[: all_migration_ids.index(migration_id)]:
            if self.__migration_statuses[m]["status"] != Status.COMPLETED:
                return RunResult(False, RunReason.NEEDS_EARLIER_MIGRATIONS)

        return RunResult(True)

    def can_reverse(self, migration_id: str) -> ReverseResult:
        """
        Covers the following cases:
        * no reversing a migration that is has not started
        * no reversing a migration that has a subsequent migration that has not been run
        """
        all_migration_ids = self.__all_migration_ids
        if self.__migration_statuses[migration_id]["status"] == Status.NOT_STARTED:
            return ReverseResult(False, ReverseReason.NOT_RUN_YET)

        for m in all_migration_ids[all_migration_ids.index(migration_id) + 1 :]:
            if self.__migration_statuses[m]["status"] != Status.NOT_STARTED:
                return ReverseResult(False, ReverseReason.NEEDS_SUBSEQUENT_MIGRATIONS)

        return ReverseResult(True)


class PolicyChecker(Checker):
    """
    The PolicyChecker validates whether you can run or
    reverse a migration based on the policy for the
    migration's group.

    Policies are defined in the ADMIN_ALLOWED_MIGRATION_GROUPS
    setting.
    """

    def __init__(self, migration_group: MigrationGroup) -> None:
        self.__migration_group: MigrationGroup = migration_group
        self.__policy: MigrationPolicy = get_migration_group_polices()[
            migration_group.value
        ]

    def _get_migration_key(self, migration_id: str) -> MigrationKey:
        return MigrationKey(self.__migration_group, migration_id)

    def can_run(self, migration_id: str) -> RunResult:
        key = self._get_migration_key(migration_id)
        if self.__policy.can_run(key):
            return RunResult(True)
        else:
            return RunResult(False, RunReason.RUN_POLICY)

    def can_reverse(self, migration_id: str) -> ReverseResult:
        key = self._get_migration_key(migration_id)
        if self.__policy.can_reverse(key):
            return ReverseResult(True)
        else:
            return ReverseResult(False, ReverseReason.REVERSE_POLICY)


def do_checks(
    checkers: Sequence[Checker], migration_id: str
) -> Tuple[RunResult, ReverseResult]:
    """
    Execute the can_run and can_reverse functionality
    for the checkers.

    Returns the failed Result(s) for the first
    check to fail in the sequence, otherwise returns
    a passing Result.
    """

    assert len(checkers) >= 1

    for checker in checkers:
        run_result = checker.can_run(migration_id)
        if not run_result.allowed:
            break

    for checker in checkers:
        reverse_result = checker.can_reverse(migration_id)
        if not reverse_result.allowed:
            break

    return run_result, reverse_result


def migration_checks_for_group(
    migration_group: MigrationGroup, migrations: Sequence[MigrationDetails]
) -> Sequence[MigrationData]:
    """
    Responsible for initializing the checkers to be used to
    validate whether migrations can be run/reversed for a specific
    migration group.

    Returns the list of migrations with the results of the checks
    in addition to migration details (id, status, blocking)
    """
    migration_ids: List[MigrationData] = []

    status_checker = StatusChecker(migration_group, migrations)
    policy_checker = PolicyChecker(migration_group)

    checkers = [status_checker, policy_checker]

    for details in migrations:
        run_result, reverse_result = do_checks(checkers, details.migration_id)
        migration_ids.append(
            MigrationData(
                migration_id=details.migration_id,
                status=details.status.value,
                blocking=details.blocking,
                can_run=run_result.allowed,
                can_reverse=reverse_result.allowed,
                run_reason=run_result.reason.value if run_result.reason else "",
                reverse_reason=reverse_result.reason.value
                if reverse_result.reason
                else "",
            )
        )

    return migration_ids
