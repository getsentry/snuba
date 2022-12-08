from datetime import datetime
from typing import Any, Mapping, Optional, Sequence, Tuple, TypedDict

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "search_issues.processor")


class SearchIssueData(TypedDict, total=False):
    fingerprint: Sequence[str]
    occurrence_id: str
    occurrence_type_id: int

    # occurrence-related
    event_id: Optional[str]
    trace_id: Optional[str]
    platform: str
    environment: Optional[str]
    release: Optional[str]
    dist: Optional[str]
    receive_timestamp: float
    client_timestamp: float
    tags: Mapping[str, Any]
    user: Mapping[str, Any]  # user, user_hash, user_id, user_name, user_email
    ip_address: str
    sdk: Mapping[str, Any]  # sdk_name, sdk_version
    contexts: Mapping[str, Any]
    request: Mapping[str, Any]  # http_method, http_referer


class SearchIssueEvent(TypedDict):
    # meta
    retention_days: int

    # issue-related
    organization_id: int
    project_id: int
    group_ids: Sequence[int]
    search_title: str
    detection_timestamp: float

    data: SearchIssueData


class SearchIssuesMessageProcessor(DatasetMessageProcessor):
    def process_message(
        self, message: Tuple[int, str, SearchIssueEvent], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        if not (isinstance(message, (list, tuple)) and len(message) >= 2):
            return None

        # TODO: validate version
        version = message[0]
        if not version:
            return None

        type_, event = message[1:3]
        if type_ != "insert":
            return None

        try:
            detection_timestamp = datetime.utcfromtimestamp(
                event["detection_timestamp"]
            )

            try:
                retention_days = enforce_retention(
                    event.get("retention_days", 90), detection_timestamp
                )
            except EventTooOld:
                metrics.increment("event_too_old")
                return None

            group_ids = event["group_ids"]
            # TODO: make use of event.data
            # event_data = event.get("data", {})

            processed = [
                {
                    "project_id": event["project_id"],
                    "organization_id": event["organization_id"],
                    "group_id": group_id,
                    "search_title": event["search_title"],
                    "detection_timestamp": detection_timestamp,
                    "retention_days": retention_days,
                }
                for group_id in group_ids or ()
            ]
        except IndexError:
            metrics.increment("invalid_message")
            return None
        except ValueError:
            metrics.increment("invalid_uuid")
            return None
        except KeyError:
            metrics.increment("missing_field")
            return None
        except Exception:
            metrics.increment("process_message_error")
            raise
        return InsertBatch(processed, None)
