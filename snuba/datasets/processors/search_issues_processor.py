import uuid
from datetime import datetime
from typing import Any, Mapping, MutableMapping, Optional, Sequence, Tuple, TypedDict

from snuba import environment, settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention, extract_user
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import (
    InsertBatch,
    InvalidMessageType,
    InvalidMessageVersion,
    ProcessedMessage,
    _ensure_valid_date,
    _ensure_valid_ip,
)
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.serializable_exception import SerializableException

metrics = MetricsWrapper(environment.metrics, "search_issues.processor")


class InvalidMessageFormat(SerializableException):
    pass


class IssueOccurrenceData(TypedDict, total=False):
    # issue occurrence data from event.occurrence_data
    id: str  # occurrence_id
    type: int  # occurrence_type
    event_id: str
    fingerprint: Sequence[str]
    issue_title: str
    subtitle: str
    resource_id: Optional[str]
    detection_time: float


class IssueEventData(TypedDict, total=False):
    # general data from event.data map
    trace_id: Optional[str]
    received: float
    client_timestamp: float
    tags: Mapping[str, Any]
    user: Mapping[str, Any]  # user_hash, user_id, user_name, user_email, ip_address
    sdk: Mapping[str, Any]  # sdk_name, sdk_version
    contexts: Mapping[str, Any]
    request: Mapping[str, Any]  # http_method, http_referer

    # tag aliases
    # tags[environment] -> environment
    # tags[sentry:release] -> release
    # tags[sentry:dist] -> dist
    # tags[sentry:user] -> user


class SearchIssueEvent(TypedDict, total=False):
    # meta
    retention_days: int

    # issue-related
    organization_id: int
    project_id: int
    event_id: str
    group_id: int
    platform: str
    primary_hash: str
    datetime: str

    data: IssueEventData
    occurrence_data: IssueOccurrenceData


def ensure_uuid(value: str) -> str:
    return str(uuid.UUID(value))


class SearchIssuesMessageProcessor(DatasetMessageProcessor):
    FINGERPRINTS_HARD_LIMIT_SIZE = 100

    def _process_user(
        self, event_data: IssueEventData, processed: MutableMapping[str, Any]
    ) -> None:
        if not event_data:
            return

        user_data: MutableMapping[str, Any] = {}

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
        detection_timestamp = datetime.utcfromtimestamp(
            event_occurrence_data["detection_time"]
        )
        receive_timestamp = datetime.utcfromtimestamp(event_data["received"])
        retention_days = enforce_retention(
            event.get("retention_days", 90), detection_timestamp
        )

        if event_data.get("client_timestamp", None):
            client_timestamp = datetime.utcfromtimestamp(event_data["client_timestamp"])
        else:
            if not event.get("datetime"):
                raise InvalidMessageFormat(
                    "message missing data.client_timestamp or datetime field"
                )

            _client_timestamp = _ensure_valid_date(
                datetime.strptime(event["datetime"], settings.PAYLOAD_DATETIME_FORMAT)
            )
            if _client_timestamp is None:
                raise InvalidMessageFormat(
                    f"datetime field has incompatible datetime format: expected({settings.PAYLOAD_DATETIME_FORMAT}), got ({event['datetime']})"
                )
            client_timestamp = _client_timestamp

        fingerprints = event_occurrence_data["fingerprint"]
        fingerprints = fingerprints[: self.FINGERPRINTS_HARD_LIMIT_SIZE - 1]

        fields: MutableMapping[str, Any] = {
            "organization_id": event["organization_id"],
            "project_id": event["project_id"],
            "event_id": ensure_uuid(event["event_id"]),
            "search_title": event_occurrence_data["issue_title"],
            "primary_hash": ensure_uuid(event["primary_hash"]),
            "fingerprint": fingerprints,
            "occurrence_id": ensure_uuid(event_occurrence_data["id"]),
            "occurrence_type_id": event_occurrence_data["type"],
            "detection_timestamp": detection_timestamp,
            "receive_timestamp": receive_timestamp,
            "client_timestamp": client_timestamp,
            # TODO: fix the below field assignments to actually extract from event data
            "platform": "platform",
            "environment": "environment",
            "release": "release",
            "dist": "dist",
            "contexts.key": [],
            "contexts.value": [],
            "tags.key": [],
            "tags.value": [],
        }

        # optional fields
        self._process_user(event_data, fields)

        return [
            {
                "group_id": event["group_id"],
                **fields,
                "message_timestamp": metadata.timestamp,
                "retention_days": retention_days,
                "partition": metadata.partition,
                "offset": metadata.offset,
            }
        ]

    def process_message(
        self, message: Tuple[int, str, SearchIssueEvent], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        if not (isinstance(message, (list, tuple)) and len(message) >= 2):
            raise InvalidMessageFormat(
                f"Expected message format (<version:int>, <operation:str>, <event>>)), got {message} instead"
            )

        version = message[0]
        if not version or version != 2:
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
