from __future__ import annotations

from functools import wraps
from typing import Any, Callable, Dict

from flask import Response, jsonify, make_response
from werkzeug.routing import BaseConverter

from snuba import settings
from snuba.migrations.policies import (
    AllMigrationsPolicy,
    MigrationPolicy,
    NoMigrationsPolicy,
    NonBlockingMigrationsPolicy,
)
from snuba.migrations.runner import MigrationKey

policy_map = {
    "AllMigrationsPolicy": AllMigrationsPolicy(),
    "NoMigrationsPolicy": NoMigrationsPolicy(),
    "NonBlockingMigrationsPolicy": NonBlockingMigrationsPolicy(),
}


class MigrationActionConverter(BaseConverter):
    regex = r"(?:run|reverse)"


def check_migration_perms(f: Callable[..., Response]) -> Callable[..., Response]:
    @wraps(f)
    def check_group_perms(*args: Any, **kwargs: Any) -> Response:
        ADMIN_ALLOWED_MIGRATION_GROUPS: Dict[str, MigrationPolicy] = {
            k: policy_map[v] for k, v in settings.ADMIN_ALLOWED_MIGRATION_GROUPS.items()
        }
        group = kwargs["group"]
        if group not in ADMIN_ALLOWED_MIGRATION_GROUPS:

            return make_response(jsonify({"error": "Group not allowed"}), 400)

        if "action" in kwargs:
            action = kwargs["action"]
            migration_id = kwargs["migration_id"]
            migration_key = MigrationKey(group, migration_id)
            policy = ADMIN_ALLOWED_MIGRATION_GROUPS[group]
            if action == "run":
                if not policy.can_run(migration_key):
                    return make_response(
                        jsonify({"error": "Group not allowed run policy"}), 400
                    )
            elif action == "reverse":
                if not policy.can_reverse(migration_key):
                    return make_response(
                        jsonify({"error": "Group not allowed reverse policy"}), 400
                    )
        return f(*args, **kwargs)

    return check_group_perms
