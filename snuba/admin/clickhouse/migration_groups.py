from typing import List, Sequence, Tuple, TypedDict

from snuba.migrations.runner import MigrationDetails, MigrationKey, Runner

runner = Runner()
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

from snuba.admin.migrations_policies import ADMIN_ALLOWED_MIGRATION_GROUPS
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.policies import MigrationPolicy
from snuba.migrations.status import Status


class Action(Enum):
    RUN = "run"
    REVERSE = "reverse"


class Reason(Enum):
    NO_REASON_NEEDED = ""
    ALREADY_RUN = "already run"
    NOT_RUN_YET = "can't reverse if not already run"
    NEEDS_SUBSEQUENT_MIGRATIONS = "subsequent migrations must be reversed first"
    NEEDS_EARLIER_MIGRATIONS = "earlier migrations must run first"
    RUN_POLICY = "group not allowed run policy"
    REVERSE_POLICY = "group not allowed reverse policy"


@dataclass
class ActionReason:
    action: Action
    allowed: bool
    reason: Reason


class MigrationData(TypedDict):
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
    @abstractmethod
    def check_can_run(self, migration_id: str) -> ActionReason:
        raise NotImplementedError

    @abstractmethod
    def check_can_reverse(self, migration_id: str) -> ActionReason:
        raise NotImplementedError


class StatusChecks(Checker):
    def __init__(
        self,
        migration_group: MigrationGroup,
        migration_details: Sequence[MigrationDetails],
    ) -> None:
        self.__group = migration_group
        migration_statuses = {}
        for migration_id, status, blocking in migration_details:
            migration_statuses[migration_id] = {
                "migration_id": migration_id,
                "status": status,
                "blocking": blocking,
            }
        self.__migration_statuses = migration_statuses

    @property
    def all_migration_ids(self) -> Sequence[str]:
        return list(self.__migration_statuses.keys())

    def check_can_run(self, migration_id: str) -> ActionReason:
        all_migration_ids = self.all_migration_ids
        if self.__migration_statuses[migration_id]["status"] != Status.NOT_STARTED:
            return ActionReason(Action.RUN, False, Reason.ALREADY_RUN)

        for m in all_migration_ids[: all_migration_ids.index(migration_id)]:
            if self.__migration_statuses[m]["status"] != Status.COMPLETED:
                return ActionReason(Action.RUN, False, Reason.NEEDS_EARLIER_MIGRATIONS)

        return ActionReason(Action.RUN, True, Reason.NO_REASON_NEEDED)

    def check_can_reverse(self, migration_id: str) -> ActionReason:
        all_migration_ids = self.all_migration_ids
        if self.__migration_statuses[migration_id]["status"] == Status.NOT_STARTED:
            return ActionReason(Action.REVERSE, False, Reason.NOT_RUN_YET)

        for m in all_migration_ids[all_migration_ids.index(migration_id) + 1 :]:
            if self.__migration_statuses[m]["status"] != Status.NOT_STARTED:
                return ActionReason(
                    Action.REVERSE, False, Reason.NEEDS_SUBSEQUENT_MIGRATIONS
                )

        return ActionReason(Action.REVERSE, True, Reason.NO_REASON_NEEDED)


class PolicyChecks(Checker):
    def __init__(self, migration_group: MigrationGroup) -> None:
        self.__migration_group: MigrationGroup = migration_group
        self.__policy: MigrationPolicy = ADMIN_ALLOWED_MIGRATION_GROUPS[
            migration_group.value
        ]

    def _get_migration_key(self, migration_id: str) -> MigrationKey:
        return MigrationKey(self.__migration_group, migration_id)

    def check_can_run(self, migration_id: str) -> ActionReason:
        key = self._get_migration_key(migration_id)
        if self.__policy.can_run(key):
            return ActionReason(Action.RUN, True, Reason.NO_REASON_NEEDED)
        else:
            return ActionReason(Action.RUN, False, Reason.RUN_POLICY)

    def check_can_reverse(self, migration_id: str) -> ActionReason:
        key = self._get_migration_key(migration_id)
        if self.__policy.can_reverse(key):
            return ActionReason(Action.REVERSE, True, Reason.NO_REASON_NEEDED)
        else:
            return ActionReason(Action.REVERSE, False, Reason.REVERSE_POLICY)


def checks_for_group(
    migration_group: MigrationGroup, migration_details: Sequence[MigrationDetails]
) -> Sequence[MigrationData]:
    migration_ids: List[MigrationData] = []

    status_checker = StatusChecks(migration_group, migration_details)
    policy_checker = PolicyChecks(migration_group)

    checkers = [status_checker, policy_checker]

    for details in migration_details:
        run_result, reverse_result = do_checks(checkers, details.migration_id)
        migration_ids.append(format_migration_data(details, run_result, reverse_result))

    return migration_ids


def do_checks(
    checkers: Sequence[Checker], migration_id: str
) -> Tuple[ActionReason, ActionReason]:

    assert len(checkers) >= 1

    for checker in checkers:
        run_result = checker.check_can_run(migration_id)
        if not run_result.allowed:
            break

    for checker in checkers:
        reverse_result = checker.check_can_reverse(migration_id)
        if not reverse_result.allowed:
            break

    return run_result, reverse_result


def format_migration_data(
    details: MigrationDetails,
    run_result: ActionReason,
    reverse_result: ActionReason,
) -> MigrationData:
    return {
        "migration_id": details.migration_id,
        "status": details.status.value,
        "blocking": details.blocking,
        "can_run": run_result.allowed,
        "run_reason": run_result.reason.value,
        "can_reverse": reverse_result.allowed,
        "reverse_reason": reverse_result.reason.value,
    }


def get_migration_ids_data(
    groups: Sequence[str],
) -> Sequence[MigrationGroupPayload]:
    result = []
    for group, migration_details in runner.show_all(groups):
        migration_ids = checks_for_group(group, migration_details)
        payload: MigrationGroupPayload = {
            "group": group.value,
            "migration_ids": migration_ids,
        }
        result.append(payload)
    return result
