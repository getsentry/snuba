"""
Sentry Logs for each stage of a delete, so a delete can be followed per
project in Sentry Logs. Every event is named `snuba.delete.<state>`.

Only issue deletes on the errors replacer (`end_delete_groups`) and deletes on
the EAP storages are logged.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from sentry_sdk import logger as sentry_logger

from snuba.datasets.storages.storage_key import StorageKey
from snuba.lw_deletions.types import AttributeConditions

logger = logging.getLogger(__name__)

# Each stage has its own done / failed / skipped events.
DeleteState = Literal[
    "received",
    "redis_written",
    "redis_failed",
    "redis_skipped",
    "errors_written",
    "errors_failed",
    "errors_skipped",
    "eap_deleted",
    "eap_failed",
    "eap_skipped",
]
DeleteTarget = Literal["errors", "redis", "eap"]
AttributeValue = str | int | float | bool | None

MAX_GROUP_IDS = 10
MAX_ERROR_MESSAGE_LENGTH = 200
# Built from the values: StorageKey members are only registered at runtime.
EAP_STORAGE_KEYS = frozenset({StorageKey("eap_items"), StorageKey("eap_items_2")})
# The EAP attribute an issue delete filters on.
EAP_GROUP_ID_ATTRIBUTE = "group_id"


def log_delete_event(
    state: DeleteState,
    target: DeleteTarget,
    *,
    project_id: int | None = None,
    organization_id: int | None = None,
    group_ids: Sequence[Any] = (),
    transaction_id: str | None = None,
    reason: str | None = None,
    error: BaseException | None = None,
    **extra: AttributeValue,
) -> None:
    """
    Send one `snuba.delete.<state>` Sentry Log. `*_failed` events are sent at
    error level, the others at info level.

    This never raises, so a logging failure can't change how the delete runs
    or hide the delete's own exception.
    """
    try:
        attributes: dict[str, AttributeValue] = {
            "target": target,
            "project_id": project_id,
            "organization_id": organization_id,
            "group_ids_count": len(group_ids),
            "group_ids": ",".join(str(g) for g in group_ids[:MAX_GROUP_IDS]) or None,
            "transaction_id": transaction_id,
            "reason": reason,
            **extra,
        }
        if error is not None:
            attributes["error"] = type(error).__name__
            attributes["error_message"] = _error_message(error)

        send = sentry_logger.error if state.endswith("_failed") else sentry_logger.info
        send(
            f"snuba.delete.{state}",
            attributes={k: v for k, v in attributes.items() if v is not None},
        )
    except Exception:
        # Not logger.exception: ERROR records become Sentry issues.
        logger.warning("Could not send the snuba.delete.%s log", state, exc_info=True)


def _error_message(error: BaseException) -> str | None:
    # Some exceptions can't be turned into a string; send the event without it.
    try:
        return str(error)[:MAX_ERROR_MESSAGE_LENGTH]
    except Exception:
        return None


def eap_group_ids(attribute_conditions: AttributeConditions | None) -> Sequence[Any]:
    """The issue IDs an EAP delete filters on, or nothing for other deletes."""
    if attribute_conditions is None:
        return []
    _, values = attribute_conditions.attributes.get(EAP_GROUP_ID_ATTRIBUTE, (None, []))
    return values


def log_eap_delete_event(
    storage_key: StorageKey,
    state: DeleteState,
    column_conditions: Mapping[str, Sequence[Any]],
    group_ids: Sequence[Any],
    **kwargs: Any,
) -> None:
    """
    Send one event per organization and project in an EAP delete's conditions,
    so each event can be filtered by a single project. Deletes on other
    storages send nothing.
    """
    if storage_key not in EAP_STORAGE_KEYS:
        return
    for organization_id in column_conditions.get("organization_id") or [None]:
        for project_id in column_conditions.get("project_id") or [None]:
            log_delete_event(
                state,
                "eap",
                organization_id=organization_id,
                project_id=project_id,
                group_ids=group_ids,
                **kwargs,
            )
