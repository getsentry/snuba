from typing import Any, Dict, List, Optional, Union

from snuba import settings
from snuba.admin.audit_log.action import (
    MIGRATION_ACTIONS,
    RUNTIME_CONFIG_ACTIONS,
    AuditLogAction,
)


def build_blocks(
    data: Any, action: AuditLogAction, timestamp: str, user: str
) -> List[Any]:
    if action in RUNTIME_CONFIG_ACTIONS:
        text = build_runtime_config_text(data, action)
    elif action in MIGRATION_ACTIONS:
        text = build_migration_run_text(data, action)
    else:
        text = action.value

    section = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": text},
    }

    return [section, build_context(user, timestamp, action)]


def build_runtime_config_text(data: Any, action: AuditLogAction) -> Optional[str]:
    base = "*Runtime Config Option:*"
    removed = f"~```{{'{data['option']}': {data.get('old')}}}```~"
    added = f"```{{'{data['option']}': {data.get('new')}}}```"
    updated = f"{removed} {added}"

    if action == AuditLogAction.REMOVED_OPTION:
        return f"{base} :put_litter_in_its_place:\n\n {removed}"
    elif action == AuditLogAction.ADDED_OPTION:
        return f"{base} :new:\n\n {added}"
    elif action == AuditLogAction.UPDATED_OPTION:
        return f"{base} :up: :date:\n\n {updated}"
    else:
        # todo: raise error, cause slack won't accept this
        # if it is none
        return None


def build_migration_run_text(data: Any, action: AuditLogAction) -> Optional[str]:
    if action == AuditLogAction.RAN_MIGRATION:
        action_text = f":runner: ran migration {data['migration']}"
    elif action == AuditLogAction.REVERSED_MIGRATION:
        action_text = f":uno-reverse: reversed migration {data['migration']}"
    else:
        return None
    return f":warning: *Migration:* \n\n{action_text} (force={data['force']}, fake={data['fake']})"


def build_context(
    user: str, timestamp: str, action: AuditLogAction
) -> Dict[str, Union[str, List[Dict[str, str]]]]:
    url = f"{settings.ADMIN_URL}/#auditlog"
    return {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"{action.value} at *<{url}|{timestamp}>* by *<{user}>*",
            }
        ],
    }
