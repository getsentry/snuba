from __future__ import annotations

from collections import defaultdict
from functools import wraps
from typing import Any, Callable, Dict, List, MutableMapping, Sequence

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
from snuba.migrations.policies import MigrationPolicy
from snuba.migrations.runner import MigrationKey, get_active_migration_groups

ACTIONS_TO_POLICIES = {
    ExecuteAllAction: "AllMigrationsPolicy",
    ExecuteNonBlockingAction: "NonBlockingMigrationsPolicy",
    ExecuteNoneAction: "NoMigrationsPolicy",
}


def get_migration_group_policies(
    user: AdminUser,
) -> Dict[str, Sequence[MigrationPolicy]]:
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
    group_policies: MutableMapping[str, List[str]] = defaultdict(list)
    allowed_groups = [
        group.value
        for group in get_active_migration_groups()
        if group.value in settings.ADMIN_ALLOWED_MIGRATION_GROUPS
    ]

    for role in user.roles:
        for action in role.actions:
            if not isinstance(action, MigrationAction):
                continue

            for resource in action._resources:
                group = resource.name
                print("nammmmee", group)
                if group in allowed_groups:
                    group_policies[group].append(ACTIONS_TO_POLICIES[action.__class__])

    print("-----\n\n")
    print("GROUP POLICES", group_policies)
    print("ALLWOED GROUPS", allowed_groups)
    print("-----\n\n")
    return {
        group: [MigrationPolicy.class_from_name(policy)() for policy in policies]
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
