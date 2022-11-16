from typing import Any, Dict, List, Optional, Union

from snuba import settings
from snuba.admin.notifications.base import NotificationAction


def build_blocks(data: Any, action: str, timestamp: str, user: str) -> List[Any]:
    return [build_section(data, action), build_context(user, timestamp, action)]


def build_text(data: Any, action: str) -> Optional[str]:
    base = "*Runtime Config Option:*"
    removed = f"~```{{'{data['option']}': {data['old']}}}```~"
    added = f"```{{'{data['option']}': {data['new']}}}```"
    updated = f"{removed} {added}"

    if action == NotificationAction.CONFIG_OPTION_REMOVED:
        return f"{base} :put_litter_in_its_place:\n\n {removed}"
    elif action == NotificationAction.CONFIG_OPTION_ADDED:
        return f"{base} :new:\n\n {added}"
    elif action == NotificationAction.CONFIG_OPTION_UPDATED:
        return f"{base} :up: :date:\n\n {updated}"
    else:
        # todo: raise error, cause slack won't accept this
        # if it is none
        return None


def build_section(data: Any, action: str) -> Any:
    text = build_text(data, action)
    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": text},
    }


def build_context(
    user: str, timestamp: str, action: str
) -> Dict[str, Union[str, List[Dict[str, str]]]]:
    url = f"{settings.ADMIN_URL}/#auditlog"
    return {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"{action} at *<{url}|{timestamp}>* by *<{user}>*",
            }
        ],
    }
