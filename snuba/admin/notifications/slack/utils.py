from typing import Any, Dict, List, Optional, Union

from snuba import settings
from snuba.admin.audit_log.action import (
    ALLOCATION_POLICY_ACTIONS,
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
    elif action in ALLOCATION_POLICY_ACTIONS:
        text = build_allocation_policy_changed_text(data, action)
    else:
        text = f"{action.value}: {data}"

    section = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": text},
    }

    return [section, build_context(user, timestamp, action)]


def build_allocation_policy_changed_text(
    data: Any, action: AuditLogAction
) -> Optional[str]:
    base = f"*Storage {data['storage']} Allocation Policy Changed:*"

    if action == AuditLogAction.ALLOCATION_POLICY_DELETE:
        removed = f"~```'{data['key']}-{data['policy']}({data.get('params', {})})'```~"
        return f"{base} :put_litter_in_its_place:\n\n{removed}"
    elif action == AuditLogAction.ALLOCATION_POLICY_UPDATE:
        updated = f"```'{data['key']}-{data['policy']}({data.get('params', {})})' = '{data['value']}'```"
        return f"{base} :up: :date:\n\n{updated}"
    else:
        # todo: raise error, cause slack won't accept this
        # if it is none
        return f"{action.value}: {data}"


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

    text = (
        f"*Migration:* \n\n{action_text} (force={data['force']}, fake={data['fake']})"
    )

    if action in [
        AuditLogAction.REVERSED_MIGRATION_FAILED,
        AuditLogAction.RAN_MIGRATION_FAILED,
    ]:
        return f":bangbang: *[FAILED]* :bangbang: {text}"

    return f":warning: {text}"


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
