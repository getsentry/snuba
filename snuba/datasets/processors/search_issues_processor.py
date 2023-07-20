import numbers
import uuid
from datetime import datetime
from typing import (
    Any,
    Dict,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
    cast,
)

from snuba import environment, settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import (
    EventTooOld,
    enforce_retention,
    extract_extra_contexts,
    extract_extra_tags,
    extract_http,
    extract_user,
)
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import (
    InsertBatch,
    InvalidMessageType,
    InvalidMessageVersion,
    ProcessedMessage,
    _as_dict_safe,
    _ensure_valid_date,
    _ensure_valid_ip,
    _unicodify,
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
    culprit: str
    level: str
    resource_id: Optional[str]
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
    environment: Optional[str]  # tags[environment] -> environment
    release: Optional[str]  # tags[sentry:release] -> release
    dist: Optional[str]  # tags[sentry:dist] -> dist
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
    platform: str
    primary_hash: str
    message: str
    datetime: str

    data: IssueEventData
    occurrence_data: IssueOccurrenceData


def ensure_uuid(value: str) -> str:
    return str(uuid.UUID(value))


class SearchIssuesMessageProcessor(DatasetMessageProcessor):
    FINGERPRINTS_HARD_LIMIT_SIZE = 100

    PROMOTED_TAGS = {
        "environment",
        "sentry:release",
        "sentry:user",
        "sentry:dist",
    }

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

    def _process_tags(
        self, event_data: IssueEventData, processed: MutableMapping[str, Any]
    ) -> None:
        existing_tags = event_data.get("tags", None)
        tags: Mapping[str, Any] = _as_dict_safe(cast(Dict[str, Any], existing_tags))
        if not existing_tags:
            processed["tags.key"], processed["tags.value"] = [], []
        else:
            processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)

        promoted_tags = {col: tags[col] for col in self.PROMOTED_TAGS if col in tags}
        processed["release"] = promoted_tags.get(
            "sentry:release",
            event_data.get("release"),
        )
        processed["environment"] = promoted_tags.get(
            "environment", event_data.get("environment")
        )
        processed["user"] = promoted_tags.get("sentry:user")
        processed["dist"] = _unicodify(
            promoted_tags.get("sentry:dist", event_data.get("dist")),
        )

    def _process_request_data(
        self, event_data: IssueEventData, processed: MutableMapping[str, Any]
    ) -> None:
        request = event_data.get("request", {})
        http_data: MutableMapping[str, Any] = {}
        extract_http(http_data, request)
        processed["http_method"] = http_data["http_method"]
        processed["http_referer"] = http_data["http_referer"]

    def _process_sdk_data(
        self, event_data: IssueEventData, processed: MutableMapping[str, Any]
    ) -> None:
        sdk = event_data.get("sdk", None) or {}
        processed["sdk_name"] = _unicodify(sdk.get("name"))
        processed["sdk_version"] = _unicodify(sdk.get("version"))

    def _process_contexts(
        self, event_data: IssueEventData, processed: MutableMapping[str, Any]
    ) -> None:
        contexts = event_data.get("contexts", {})

        processed["contexts.key"], processed["contexts.value"] = extract_extra_contexts(
            contexts
        )

        # promote fields within contexts to a top-level column
        trace = contexts.get("trace", {})
        if trace.get("trace_id") is not None:
            trace_id = _unicodify(trace["trace_id"])
            if trace_id is not None:
                processed["trace_id"] = ensure_uuid(trace_id)

        profile = contexts.get("profile", {})
        if profile.get("profile_id") is not None:
            profile_id = _unicodify(profile["profile_id"])
            if profile_id is not None:
                processed["profile_id"] = ensure_uuid(profile_id)

        replay = contexts.get("replay", {})
        if replay.get("replay_id") is not None:
            replay_id = _unicodify(replay["replay_id"])
            if replay_id is not None:
                processed["replay_id"] = ensure_uuid(replay_id)

    def __extract_timestamp(self, field: int) -> datetime:
        # We are purposely using a naive datetime here to work with the rest of the codebase.
        # We can be confident that clients are only sending UTC dates.
        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(field))
        if timestamp is None:
            timestamp = datetime.utcnow()
        return timestamp

    def _process_transaction_duration(
        self, event_data: IssueEventData, processed: MutableMapping[str, Any]
    ) -> None:
        if isinstance(event_data.get("start_timestamp"), numbers.Number) and isinstance(
            event_data.get("timestamp"), numbers.Number
        ):
            start_ts = self.__extract_timestamp(
                int(event_data.get("start_timestamp", 0))
            )
            finish_ts = self.__extract_timestamp(int(event_data.get("timestamp", 0)))
            duration_secs = (finish_ts - start_ts).total_seconds()
            processed["transaction_duration"] = max(int(duration_secs * 1000), 0)
        else:
            processed["transaction_duration"] = 0

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
            "subtitle": event_occurrence_data.get("subtitle", None),
            "culprit": event_occurrence_data.get("culprit", None),
            "level": event_occurrence_data.get("level", None),
            "primary_hash": ensure_uuid(event["primary_hash"]),
            "fingerprint": fingerprints,
            "resource_id": event_occurrence_data.get("resource_id", None),
            "occurrence_id": ensure_uuid(event_occurrence_data["id"]),
            "occurrence_type_id": event_occurrence_data["type"],
            "detection_timestamp": detection_timestamp,
            "receive_timestamp": receive_timestamp,
            "client_timestamp": client_timestamp,
            "platform": event["platform"],
            "message": _unicodify(event["message"]),
        }

        # optional fields
        self._process_tags(
            event_data, fields
        )  # environment, release, dist, user, tags.key, tags.value
        self._process_user(
            event_data, fields
        )  # user_name, user_id, user_email, ip_address_v4/ip_address_v6
        self._process_request_data(event_data, fields)  # http_method, http_referer
        self._process_sdk_data(event_data, fields)  # sdk_name, sdk_version
        self._process_contexts(event_data, fields)  # contexts.key, contexts.value

        # start_timestamp, timestamp
        self._process_transaction_duration(event_data, fields)

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
            return None
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
