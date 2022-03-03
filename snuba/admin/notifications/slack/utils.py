from typing import Any, Dict, List, Optional, Union


def build_blocks(data: Any, action: str, timestamp: str, user: str) -> List[Any]:
    return [build_section(data, action), build_context(user, timestamp, action)]


def build_text(data: Any, action: str) -> Optional[str]:
    base = "*Runtime Config Option:*"
    removed = f"~```{{'{data['option']}': {data['old']}}}```~"
    added = f"```{{'{data['option']}': {data['new']}}}```"
    updated = f"{removed} {added}"

    if action == "removed":
        return f"{base} :put_litter_in_its_place:\n\n {removed}"
    elif action == "added":
        return f"{base} :new:\n\n {added}"
    elif action == "updated":
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
        "accessory": {
            "type": "button",
            "text": {"type": "plain_text", "text": "Audit Log"},
            "url": "https://snuba-admin.getsentry.net/#auditlog",
        },
    }


def build_context(
    user: str, timestamp: str, action: str
) -> Dict[str, Union[str, List[Dict[str, str]]]]:
    return {
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": f"{action} at *{timestamp}* by *<{user}>*"}
        ],
    }
