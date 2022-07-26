import logging
import uuid
from datetime import datetime
from typing import Any, Mapping, MutableMapping, Optional, Tuple

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import (
    EventTooOld,
    enforce_retention,
    extract_extra_tags,
    extract_user,
)
from snuba.processor import (
    InsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _as_dict_safe,
    _ensure_valid_date,
    _ensure_valid_ip,
    _unicodify,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)

metrics = MetricsWrapper(environment.metrics, "replays.processor")
from sentry_sdk import capture_exception

EventDict = Mapping[Any, Any]
RetentionDays = int


class ReplaysProcessor(MessageProcessor):
    PROMOTED_TAGS = {
        "environment",
        "sentry:release",
        "sentry:user",
        "sentry:dist",
    }

    def __extract_timestamp(self, field: int) -> datetime:
        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(field))
        if timestamp is None:
            timestamp = datetime.utcnow()
        return timestamp

    def _structure_and_validate_message(
        self, message: Mapping[Any, Any]
    ) -> Optional[Tuple[EventDict, RetentionDays]]:

        event = message
        data = event["data"]

        try:
            # We are purposely using a naive datetime here to work with the
            # rest of the codebase. We can be confident that clients are only
            # sending UTC dates.
            retention_days = enforce_retention(
                message["retention_days"], datetime.utcfromtimestamp(data["timestamp"])
            )
        except EventTooOld:
            return None

        return event, retention_days

    def _process_base_event_values(
        self, processed: MutableMapping[str, Any], event_dict: EventDict
    ) -> MutableMapping[str, Any]:

        processed["replay_id"] = str(uuid.UUID(event_dict["replay_id"]))
        processed["project_id"] = event_dict["project_id"]

        processed["segment_id"] = event_dict["segment_id"]
        processed["trace_ids"] = event_dict["trace_ids"]

        processed["timestamp"] = self.__extract_timestamp(
            event_dict["data"]["timestamp"],
        )

        processed["platform"] = _unicodify(event_dict["platform"])
        return processed

    def _process_tags(
        self,
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:

        tags: Mapping[str, Any] = _as_dict_safe(event_dict["data"].get("tags", None))
        processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)
        promoted_tags = {col: tags[col] for col in self.PROMOTED_TAGS if col in tags}
        processed["release"] = promoted_tags.get(
            "sentry:release",
            event_dict.get("release"),
        )
        processed["environment"] = promoted_tags.get("environment")
        processed["user"] = promoted_tags.get("sentry:user", "")
        processed["dist"] = _unicodify(
            promoted_tags.get("sentry:dist", event_dict["data"].get("dist")),
        )

    def _process_user(
        self,
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:

        user_dict = (
            event_dict["data"].get(
                "user", event_dict["data"].get("sentry.interfaces.User", None)
            )
            or {}
        )

        user_data: MutableMapping[str, Any] = {}
        extract_user(user_data, user_dict)
        processed["user_name"] = user_data["username"]
        processed["user_id"] = user_data["user_id"]
        processed["user_email"] = user_data["email"]
        ip_address = _ensure_valid_ip(user_data["ip_address"])
        if ip_address:
            if ip_address.version == 4:
                processed["ip_address_v4"] = str(ip_address)
            elif ip_address.version == 6:
                processed["ip_address_v6"] = str(ip_address)

    def _process_sdk_data(
        self,
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:
        sdk = event_dict["data"].get("sdk", None) or {}
        processed["sdk_name"] = _unicodify(sdk.get("name") or "")
        processed["sdk_version"] = _unicodify(sdk.get("version") or "")

        if processed["sdk_name"] == "":
            metrics.increment("missing_sdk_name")
        if processed["sdk_version"] == "":
            metrics.increment("missing_sdk_version")

    def process_message(
        self, message: Mapping[Any, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        try:
            event_dict, retention_days = self._structure_and_validate_message(
                message
            ) or (
                None,
                None,
            )

            if not event_dict:
                return None
            processed: MutableMapping[str, Any] = {
                "retention_days": retention_days,
            }
            # The following helper functions should be able to be applied in any order.
            # At time of writing, there are no reads of the values in the `processed`
            # dictionary to inform values in other functions.
            # Ideally we keep continue that rule
            self._process_base_event_values(processed, event_dict)
            self._process_tags(processed, event_dict)
            self._process_sdk_data(processed, event_dict)
            processed["partition"] = metadata.partition
            processed["offset"] = metadata.offset

            # the following operation modifies the event_dict and is therefore *not* order-independent
            self._process_user(processed, event_dict)
            return InsertBatch([processed], None)
        except Exception as e:
            metrics.increment("consumer_error")
            capture_exception(e)
            return None
