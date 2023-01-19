from __future__ import annotations

from functools import wraps
from typing import Any, Callable, Dict, MutableMapping

from flask import Response, g, jsonify, make_response, request

from snuba import settings
from snuba.admin.auth_roles import (
    ExecuteAllAction,
    ExecuteNonBlockingAction,
    ExecuteNoneAction,
    MigrationAction,
)
from snuba.admin.user import AdminUser
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.policies import MigrationPolicy, max_policy
from snuba.migrations.runner import MigrationKey

ACTIONS_TO_POLICIES = {
    ExecuteAllAction: "AllMigrationsPolicy",
    ExecuteNonBlockingAction: "NonBlockingMigrationsPolicy",
    ExecuteNoneAction: "NoMigrationsPolicy",
}


def get_migration_group_policies(user: AdminUser) -> Dict[str, MigrationPolicy]:
    """
    Creates a mapping of migration groups to policies based on a user's
    roles. If a user has multiple roles, and the actions on those roles
    correspond to different policies for the same resource (migration
    group in this case), the highest policy (the most permissive) will be
    used for that resource.

    e.g. Take Roles A and B. Role A's action maps to "AllMigrationsPolicy".
    Role B's action maps to "NoMigrationsPolicy". Both Role A and Role B's
    actions include the same resource R. The resulting group policy map will be
    as follows: {"R": AllMigrationsPolicy()}
    """
    group_policies: MutableMapping[str, str] = {}
    allowed_groups = settings.ADMIN_ALLOWED_MIGRATION_GROUPS.keys()

    for role in user.roles:
        for action in role.actions:
            if not isinstance(action, MigrationAction):
                continue

            def assign_group_policy(group: str) -> None:
                if group not in allowed_groups:
                    return

                policy = ACTIONS_TO_POLICIES[action.__class__]

                curr_policy = group_policies.get(group)
                if curr_policy:
                    group_policies[group] = max_policy([curr_policy, policy])
                else:
                    group_policies[group] = policy

            for resource in action._resources:
                assign_group_policy(resource.name)

    return {
        group: MigrationPolicy.class_from_name(policy)()
        for group, policy in group_policies.items()
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
            policy = group_polices[group]

            def str_to_bool(s: str) -> bool:
                return s.strip().lower() == "true"

            dry_run = request.args.get("dry_run", False, type=str_to_bool)

            if not dry_run:
                if action == "run":
                    if not policy.can_run(migration_key):
                        return make_response(
                            jsonify({"error": "Group not allowed run policy"}), 403
                        )
                elif action == "reverse":
                    if not policy.can_reverse(migration_key):
                        return make_response(
                            jsonify({"error": "Group not allowed reverse policy"}), 403
                        )
        return f(*args, **kwargs)

    return check_group_perms
