from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Mapping, Optional, Sequence, Set, Tuple, Union

from snuba.migrations.groups import MigrationGroup, get_group_loader
from snuba.migrations.policies import MigrationPolicy
from snuba.migrations.runner import MigrationDetails, MigrationKey, Runner
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
    def can_run(self, migration_key: MigrationKey) -> RunResult:
        raise NotImplementedError

    @abstractmethod
    def can_reverse(self, migration_key: MigrationKey) -> ReverseResult:
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
        for migration_id, status, _, _ in migrations:
            migration_statuses[migration_id] = {
                "migration_id": migration_id,
                "status": status,
            }
        assert self.__all_migration_ids == list(
            migration_statuses.keys()
        ), "migration ids dont match"
        self.__migration_statuses = migration_statuses

    def _validate_key(self, migration_key: MigrationKey) -> None:
        group, migration_id = migration_key
        assert (
            group == self.__group
        ), f"Group {group.value} does not match {self.__group.value}"
        assert (
            migration_id in self.__all_migration_ids
        ), f"{migration_id} is not part of {self.__group.value} group"

    def can_run(self, migration_key: MigrationKey) -> RunResult:
        """
        Covers the following cases:
        * no running a migration that is already pending or completed
        * no running a migration that has a preceeding migration that is not completed
        """
        self._validate_key(migration_key)
        group, migration_id = migration_key
        all_migration_ids = self.__all_migration_ids
        if self.__migration_statuses[migration_id]["status"] != Status.NOT_STARTED:
            return RunResult(False, RunReason.ALREADY_RUN)

        for m in all_migration_ids[: all_migration_ids.index(migration_id)]:
            if self.__migration_statuses[m]["status"] != Status.COMPLETED:
                return RunResult(False, RunReason.NEEDS_EARLIER_MIGRATIONS)

        return RunResult(True)

    def can_reverse(self, migration_key: MigrationKey) -> ReverseResult:
        """
        Covers the following cases:
        * no reversing a migration that is has not started
        * no reversing a migration that has a subsequent migration that has not been run
        """
        self._validate_key(migration_key)
        group, migration_id = migration_key
        all_migration_ids = self.__all_migration_ids

        if self.__migration_statuses[migration_id]["status"] == Status.NOT_STARTED:
            return ReverseResult(False, ReverseReason.NOT_RUN_YET)

        for m in all_migration_ids[all_migration_ids.index(migration_id) + 1 :]:
            if self.__migration_statuses[m]["status"] != Status.NOT_STARTED:
                return ReverseResult(False, ReverseReason.NEEDS_SUBSEQUENT_MIGRATIONS)

        return ReverseResult(True)


def run_migration_checks_and_policies(
    group_policies: Mapping[str, Set[MigrationPolicy]], runner: Runner
) -> Sequence[Tuple[MigrationGroup, Sequence[MigrationData]]]:
    """
    Runs the policies for the given groups in addition to status
    checks for all groups.

    Returns the results of those checks along with the statuses
    for the migrations.
    """
    group_results: List[Tuple[MigrationGroup, Sequence[MigrationData]]] = []

    for group, migrations in runner.show_all([g for g in group_policies.keys()]):
        migration_ids: List[MigrationData] = []

        status_checker = StatusChecker(group, migrations)
        policies = group_policies[group.value]

        def do_checking(
            migration_key: MigrationKey,
        ) -> Tuple[RunResult, ReverseResult]:
            run_result = status_checker.can_run(migration_key)
            reverse_result = status_checker.can_reverse(migration_key)

            if run_result.allowed:
                run_result = (
                    RunResult(True)
                    if any(policy.can_run(migration_key) for policy in policies)
                    else RunResult(False, RunReason.RUN_POLICY)
                )
            if reverse_result.allowed:
                reverse_result = (
                    ReverseResult(True)
                    if any(policy.can_reverse(migration_key) for policy in policies)
                    else ReverseResult(False, ReverseReason.REVERSE_POLICY)
                )

            return run_result, reverse_result

        for details in migrations:
            run_result, reverse_result = do_checking(
                MigrationKey(group, details.migration_id)
            )
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

        group_results.append((group, migration_ids))

    return group_results
