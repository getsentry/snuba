from __future__ import annotations

from collections import defaultdict
from functools import wraps
from typing import Any, Callable, Dict, MutableMapping, Set

from flask import Response, g, jsonify, make_response, request

from snuba.admin.auth_roles import (
    ExecuteAllAction,
    ExecuteNonBlockingAction,
    ExecuteNoneAction,
    MigrationAction,
)
from snuba.admin.user import AdminUser
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.policies import MigrationPolicy
from snuba.migrations.runner import MigrationKey, get_active_migration_groups

ACTIONS_TO_POLICIES = {
    ExecuteAllAction: "AllMigrationsPolicy",
    ExecuteNonBlockingAction: "NonBlockingMigrationsPolicy",
    ExecuteNoneAction: "NoMigrationsPolicy",
}


def get_migration_group_policies(
    user: AdminUser,
) -> Dict[str, Set[MigrationPolicy]]:
    """
    Creates a mapping of migration groups to policies based on a user's
    roles.
    """
    group_policies: MutableMapping[str, Set[str]] = defaultdict(set)
    allowed_groups = [group.value for group in get_active_migration_groups()]

    for role in user.roles:
        for action in role.actions:
            if not isinstance(action, MigrationAction):
                continue

            for resource in action._resources:
                group = resource.name
                if group in allowed_groups:
                    group_policies[group].add(ACTIONS_TO_POLICIES[action.__class__])

    return {
        group: {MigrationPolicy.class_from_name(policy)() for policy in policies}
        for group, policies in group_policies.items()
    }


def check_migration_perms(f: Callable[..., Response]) -> Callable[..., Response]:
    """
    A wrapper decorator applied to endpoints handling migrations. It checks that we
    have a policy for the group in question and that the migration is allowed for
    the run/reverse policy defined.
    """

    @wraps(f)
    def check_group_perms(*args: Any, **kwargs: Any) -> Response:
        group = kwargs["group"]
        group_polices = get_migration_group_policies(g.user)
        if group not in group_polices:
            return make_response(jsonify({"error": "Group not allowed"}), 403)

        if "action" in kwargs:
            action = kwargs["action"]
            migration_id = kwargs["migration_id"]
            migration_key = MigrationKey(MigrationGroup(group), migration_id)
            policies = group_polices[group]

            def str_to_bool(s: str) -> bool:
                return s.strip().lower() == "true"

            dry_run = request.args.get("dry_run", False, type=str_to_bool)

            if not dry_run:
                if action == "run":
                    if not any(policy.can_run(migration_key) for policy in policies):
                        return make_response(
                            jsonify({"error": "Group not allowed run policy"}), 403
                        )
                elif action == "reverse":
                    if not any(
                        policy.can_reverse(migration_key) for policy in policies
                    ):
                        return make_response(
                            jsonify({"error": "Group not allowed reverse policy"}), 403
                        )
        return f(*args, **kwargs)

    return check_group_perms
