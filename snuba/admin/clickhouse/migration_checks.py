from typing import List, Sequence, Tuple, TypedDict

from snuba.migrations.runner import MigrationDetails, MigrationKey, Runner

runner = Runner()
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

from snuba.admin.migrations_policies import get_migration_group_polices
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.policies import MigrationPolicy
from snuba.migrations.status import Status


class Reason(Enum):
    NO_REASON_NEEDED = ""
    ALREADY_RUN = "already run"
    NOT_RUN_YET = "can't reverse if not already run"
    NEEDS_SUBSEQUENT_MIGRATIONS = "subsequent migrations must be reversed first"
    NEEDS_EARLIER_MIGRATIONS = "earlier migrations must run first"
    RUN_POLICY = "group not allowed run policy"
    REVERSE_POLICY = "group not allowed reverse policy"


@dataclass
class ResultReason:
    allowed: bool
    reason: Reason


@dataclass
class MigrationData:
    migration_id: str
    status: str
    blocking: bool
    can_run: bool
    can_reverse: bool
    run_reason: str
    reverse_reason: str


class MigrationGroupPayload(TypedDict):
    group: str
    migration_ids: Sequence[MigrationData]


class Checker(ABC):
    """
    A checker is used to encapsulate logic about whether
    a migration can be run or reversed based on some criteria
    or state the checker has.

    If a migration can be run/reversed, the ActionReason that
    is returned should have allowed = True and
    reason = Reason.NO_REASON_NEEDED.
    """

    @abstractmethod
    def can_run(self, migration_id: str) -> ResultReason:
        raise NotImplementedError

    @abstractmethod
    def can_reverse(self, migration_id: str) -> ResultReason:
        raise NotImplementedError


class StatusChecker(Checker):
    """
    The StatusChecker validates whether you can run or
    reverse a migration based on the statuses of the
    migration itself and the ones preceeding/following
    it depending on the check.

    Preceeding and following are based on the migration_id
    numbering order and assumes that holds.
    """

    def __init__(
        self,
        migration_group: MigrationGroup,
        migrations: Sequence[MigrationDetails],
    ) -> None:
        self.__group = migration_group
        migration_statuses = {}
        for migration_id, status, _ in migrations:
            migration_statuses[migration_id] = {
                "migration_id": migration_id,
                "status": status,
            }
        self.__migration_statuses = migration_statuses

    @property
    def all_migration_ids(self) -> Sequence[str]:
        return list(self.__migration_statuses.keys())

    def can_run(self, migration_id: str) -> ResultReason:
        """
        Covers the following cases:
        * no running a migration that is already pending or completed
        * no running a migration that has a preceeding migration that is not completed
        """
        all_migration_ids = self.all_migration_ids
        if self.__migration_statuses[migration_id]["status"] != Status.NOT_STARTED:
            return ResultReason(False, Reason.ALREADY_RUN)

        for m in all_migration_ids[: all_migration_ids.index(migration_id)]:
            if self.__migration_statuses[m]["status"] != Status.COMPLETED:
                return ResultReason(False, Reason.NEEDS_EARLIER_MIGRATIONS)

        return ResultReason(True, Reason.NO_REASON_NEEDED)

    def can_reverse(self, migration_id: str) -> ResultReason:
        """
        Covers the following cases:
        * no reversing a migration that is has not started
        * no reversing a migration that has a subsequent migration that has not been run
        """
        all_migration_ids = self.all_migration_ids
        if self.__migration_statuses[migration_id]["status"] == Status.NOT_STARTED:
            return ResultReason(False, Reason.NOT_RUN_YET)

        for m in all_migration_ids[all_migration_ids.index(migration_id) + 1 :]:
            if self.__migration_statuses[m]["status"] != Status.NOT_STARTED:
                return ResultReason(False, Reason.NEEDS_SUBSEQUENT_MIGRATIONS)

        return ResultReason(True, Reason.NO_REASON_NEEDED)


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

    def can_run(self, migration_id: str) -> ResultReason:
        key = self._get_migration_key(migration_id)
        if self.__policy.can_run(key):
            return ResultReason(True, Reason.NO_REASON_NEEDED)
        else:
            return ResultReason(False, Reason.RUN_POLICY)

    def can_reverse(self, migration_id: str) -> ResultReason:
        key = self._get_migration_key(migration_id)
        if self.__policy.can_reverse(key):
            return ResultReason(True, Reason.NO_REASON_NEEDED)
        else:
            return ResultReason(False, Reason.REVERSE_POLICY)


def do_checks(
    checkers: Sequence[Checker], migration_id: str
) -> Tuple[ResultReason, ResultReason]:

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


def checks_for_group(
    migration_group: MigrationGroup, migrations: Sequence[MigrationDetails]
) -> Sequence[MigrationData]:
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
                run_reason=run_result.reason.value,
                reverse_reason=reverse_result.reason.value,
            )
        )

    return migration_ids
