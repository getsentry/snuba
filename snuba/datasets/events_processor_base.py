import logging
from enum import Enum
from typing import Any, MutableMapping, Optional, TypedDict

logger = logging.getLogger(__name__)


class ReplacementType(str, Enum):
    START_DELETE_GROUPS = "start_delete_groups"
    START_MERGE = "start_merge"
    START_UNMERGE = "start_unmerge"
    START_UNMERGE_HIERARCHICAL = "start_unmerge_hierarchical"
    START_DELETE_TAG = "start_delete_tag"
    END_DELETE_GROUPS = "end_delete_groups"
    END_MERGE = "end_merge"
    END_UNMERGE = "end_unmerge"
    END_UNMERGE_HIERARCHICAL = "end_unmerge_hierarchical"
    END_DELETE_TAG = "end_delete_tag"
    TOMBSTONE_EVENTS = "tombstone_events"
    REPLACE_GROUP = "replace_group"
    EXCLUDE_GROUPS = "exclude_groups"


REPLACEMENT_EVENT_TYPES = frozenset(
    [
        ReplacementType.START_DELETE_GROUPS,
        ReplacementType.START_MERGE,
        ReplacementType.START_UNMERGE,
        ReplacementType.START_DELETE_TAG,
        ReplacementType.END_DELETE_GROUPS,
        ReplacementType.END_MERGE,
        ReplacementType.END_UNMERGE,
        ReplacementType.END_DELETE_TAG,
        ReplacementType.TOMBSTONE_EVENTS,
        ReplacementType.EXCLUDE_GROUPS,
        ReplacementType.REPLACE_GROUP,
    ]
)


class InsertEvent(TypedDict):
    group_id: Optional[int]
    event_id: str
    organization_id: int
    project_id: int
    message: str
    platform: str
    datetime: str  # snuba.settings.PAYLOAD_DATETIME_FORMAT
    data: MutableMapping[str, Any]
    primary_hash: str  # empty string represents None
    retention_days: int
