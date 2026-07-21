import os
from typing import Any

from snuba import settings
from snuba.admin.audit_log.action import MIGRATION_ACTIONS, AuditLogAction


def build_blocks(data: Any, action: AuditLogAction, timestamp: str, user: str) -> list[Any]:
    if action in MIGRATION_ACTIONS:
        text = build_migration_run_text(data, action)
    else:
        text = f"{action.value}: {data}"

    section = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": text},
    }

    return [section, build_context(user, timestamp, action)]


def build_migration_run_text(data: Any, action: AuditLogAction) -> str | None:
    if action in [
        AuditLogAction.RAN_MIGRATION_COMPLETED,
        AuditLogAction.RAN_MIGRATION_FAILED,
    ]:
        action_text = f":athletic_shoe: ran migration `{data['migration']}`"
    elif action in [
        AuditLogAction.REVERSED_MIGRATION_COMPLETED,
        AuditLogAction.REVERSED_MIGRATION_FAILED,
    ]:
        action_text = f":back: reversed migration `{data['migration']}`"
    else:
        return None

    text = f"*Migration:* \n\n{action_text} (force={data['force']}, fake={data['fake']})"

    if action in [
        AuditLogAction.REVERSED_MIGRATION_FAILED,
        AuditLogAction.RAN_MIGRATION_FAILED,
    ]:
        return f":bangbang: *[FAILED]* :bangbang: {text}"

    return f":warning: {text}"


def build_context(
    user: str, timestamp: str, action: AuditLogAction
) -> dict[str, str | list[dict[str, str]]]:
    url = settings.ADMIN_URL
    environ = os.environ.get("SENTRY_ENVIRONMENT") or "unknown environment"
    return {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"{action.value} at *<{url}|{timestamp}>* by *<{user}>* in *<{environ}>*",
            }
        ],
    }
