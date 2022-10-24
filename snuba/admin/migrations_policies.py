from __future__ import annotations

from functools import wraps
from typing import Any, Callable, Dict

from flask import Response, jsonify, make_response, request
from werkzeug.routing import BaseConverter

from snuba import settings
from snuba.migrations.policies import (
    AllMigrationsPolicy,
    MigrationPolicy,
    NoMigrationsPolicy,
    NonBlockingMigrationsPolicy,
)
from snuba.migrations.runner import MigrationKey


class MigrationActionConverter(BaseConverter):
    regex = r"(?:run|reverse)"


def check_migration_perms(f: Callable[..., Response]) -> Callable[..., Response]:
    """
    A wrapper decorator applied to endpoints handling migrations. It checks that
    the migration group is in the ADMIN_ALLOWED_MIGRATION_GROUPS setting and checks
    that the migration has the appropriate run/reverse policy
    """

    @wraps(f)
    def check_group_perms(*args: Any, **kwargs: Any) -> Response:
        policy_map = {
            "AllMigrationsPolicy": AllMigrationsPolicy(),
            "NoMigrationsPolicy": NoMigrationsPolicy(),
            "NonBlockingMigrationsPolicy": NonBlockingMigrationsPolicy(),
        }
        ADMIN_ALLOWED_MIGRATION_GROUPS: Dict[str, MigrationPolicy] = {
            k: policy_map[v] for k, v in settings.ADMIN_ALLOWED_MIGRATION_GROUPS.items()
        }
        group = kwargs["group"]
        if group not in ADMIN_ALLOWED_MIGRATION_GROUPS:
            return make_response(jsonify({"error": "Group not allowed"}), 403)

        if "action" in kwargs:
            action = kwargs["action"]
            migration_id = kwargs["migration_id"]
            migration_key = MigrationKey(group, migration_id)
            policy = ADMIN_ALLOWED_MIGRATION_GROUPS[group]

            def str_to_bool(s: str) -> bool:
                return s.strip().lower() in ("yes", "true", "t", "1")

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
