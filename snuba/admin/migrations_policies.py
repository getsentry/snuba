from __future__ import annotations

from functools import wraps
from typing import Any, Callable, Dict

from flask import Response, jsonify, make_response, request

from snuba import settings
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.policies import MigrationPolicy
from snuba.migrations.runner import MigrationKey


def get_migration_group_polices() -> Dict[str, MigrationPolicy]:
    """
    Maps migration groups to their policies if as defined by the
    ADMIN_ALLOWED_MIGRATION_GROUPS setting. If a group is not defined
    it means no access at all to that group through admin.
    """
    return {
        group_name: MigrationPolicy.class_from_name(policy_name)()
        for group_name, policy_name in settings.ADMIN_ALLOWED_MIGRATION_GROUPS.items()
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
        group_polices = get_migration_group_polices()
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
