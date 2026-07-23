from collections.abc import Mapping, Sequence
from typing import Any, TypedDict

from snuba.datasets.processors.rust_compat_processor import RustCompatProcessor


class IssueOccurrenceData(TypedDict, total=False):
    # issue occurrence data from event.occurrence_data
    id: str  # occurrence_id
    type: int  # occurrence_type
    event_id: str
    fingerprint: Sequence[str]
    issue_title: str
    subtitle: str
    culprit: str
    level: str
    resource_id: str | None
    detection_time: float


class IssueEventData(TypedDict, total=False):
    # general data from event.data map
    received: float
    client_timestamp: float
    timestamp: float
    start_timestamp: float

    tags: Mapping[str, Any]
    user: Mapping[str, Any]  # user_hash, user_id, user_name, user_email, ip_address
    sdk: Mapping[str, Any]  # sdk_name, sdk_version
    contexts: Mapping[str, Any]
    request: Mapping[str, Any]  # http_method, http_referer

    # tag aliases
    environment: str | None  # tags[environment] -> environment
    release: str | None  # tags[sentry:release] -> release
    dist: str | None  # tags[sentry:dist] -> dist
    # (tags[sentry:user] or user[id]) -> user

    # contexts aliases
    # contexts.trace.trace_id -> trace_id


class SearchIssueEvent(TypedDict, total=False):
    # meta
    retention_days: int

    # issue-related
    organization_id: int
    project_id: int
    event_id: str
    group_id: int
    group_first_seen: str
    platform: str
    primary_hash: str
    message: str
    datetime: str

    data: IssueEventData
    occurrence_data: IssueOccurrenceData


class SearchIssuesMessageProcessor(RustCompatProcessor):
    def __init__(self) -> None:
        super().__init__("SearchIssuesMessageProcessor")
