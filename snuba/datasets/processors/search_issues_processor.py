from datetime import datetime
from typing import Any, Mapping, MutableMapping, Optional, Sequence, Tuple, TypedDict

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention, extract_user
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import (
    InsertBatch,
    InvalidMessageType,
    InvalidMessageVersion,
    ProcessedMessage,
    _ensure_valid_ip,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "search_issues.processor")


class IssueOccurrenceData(TypedDict, total=False):
    # issue occurrence data from event.occurrence_data
    id: str  # occurrence_id
    type: int  # occurrence_type
    event_id: str
    fingerprint: Sequence[str]
    issue_title: str
    subtitle: str
    resource_id: Optional[str]
    detection_time: str


class IssueEventData(TypedDict, total=False):
    # general data from event.data map
    trace_id: Optional[str]
    platform: str
    environment: Optional[str]
    release: Optional[str]
    dist: Optional[str]
    receive_timestamp: float
    client_timestamp: float
    tags: Mapping[str, Any]
    user: Mapping[str, Any]  # user, user_hash, user_id, user_name, user_email
    ip_address: str  # user.ip_address
    sdk: Mapping[str, Any]  # sdk_name, sdk_version
    contexts: Mapping[str, Any]
    request: Mapping[str, Any]  # http_method, http_referer


class SearchIssueEvent(TypedDict):
    # meta
    retention_days: int

    # issue-related
    organization_id: int
    project_id: int
    group_id: int  # backwards compatibility
    group_ids: Sequence[int]

    data: IssueEventData
    occurrence_data: IssueOccurrenceData


class SearchIssuesMessageProcessor(DatasetMessageProcessor):
    def _process_user(
        self, event_data: IssueEventData, processed: MutableMapping[str, Any]
    ) -> None:
        if not event_data:
            return

        user_data = {}

        extract_user(user_data, event_data.get("user", {}))
        processed["user_name"] = user_data["username"]
        processed["user_id"] = user_data["user_id"]
        processed["user_email"] = user_data["email"]

        ip_address = _ensure_valid_ip(user_data["ip_address"])
        if ip_address:
            if ip_address.version == 4:
                processed["ip_address_v4"] = str(ip_address)
            elif ip_address.version == 6:
                processed["ip_address_v6"] = str(ip_address)

        return

    def process_insert_v1(
        self, event: SearchIssueEvent, metadata: KafkaMessageMetadata
    ) -> Sequence[Mapping[str, Any]]:
        event_data = event["data"]
        event_occurrence_data = event["occurrence_data"]

        # required fields
        detection_timestamp = datetime.fromtimestamp(
            event_occurrence_data["detection_time"]
        )
        retention_days = enforce_retention(
            event.get("retention_days", 90), detection_timestamp
        )
        fields: MutableMapping[str, Any] = {
            "organization_id": event["organization_id"],
            "project_id": event["project_id"],
            "search_title": event_occurrence_data["issue_title"],
            "fingerprint": event_occurrence_data["fingerprint"],
            "occurrence_id": event_occurrence_data["id"],
            "occurrence_type_id": event_occurrence_data["type"],
            "detection_timestamp": detection_timestamp,
            # TODO: fix the below field assignments to actually extract from event data
            "receive_timestamp": detection_timestamp,
            "client_timestamp": detection_timestamp,
            "platform": "platform",
            "contexts.key": [],
            "contexts.value": [],
            "tags.key": [],
            "tags.value": [],
        }

        # optional fields
        self._process_user(event_data, fields)

        return [
            {
                "group_id": group_id,
                **fields,
                "message_timestamp": metadata.timestamp,
                "retention_days": retention_days,
                "partition": metadata.partition,
                "offset": metadata.offset,
            }
            for group_id in event["group_ids"]
        ]

    def process_message(
        self, message: Tuple[int, str, SearchIssueEvent], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        if not (isinstance(message, (list, tuple)) and len(message) >= 2):
            return None

        version = message[0]
        if not version or version != 1:
            metrics.increment("invalid_message_version")
            raise InvalidMessageVersion(f"Unsupported message version: {version}")

        type_, event = message[1:3]
        if type_ != "insert":
            metrics.increment("invalid_message_type")
            raise InvalidMessageType(f"Invalid message type: {type_}")

        try:
            processed = self.process_insert_v1(event, metadata)
        except EventTooOld:
            metrics.increment("event_too_old")
            raise
        except IndexError:
            metrics.increment("invalid_message")
            raise
        except ValueError:
            metrics.increment("invalid_uuid")
            raise
        except KeyError:
            metrics.increment("missing_field")
            raise
        except Exception:
            metrics.increment("process_message_error")
            raise
        return InsertBatch(processed, None)
