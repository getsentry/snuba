import logging
import uuid
from datetime import datetime
from hashlib import md5
from typing import Any, Mapping, MutableMapping, Optional, cast

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
from snuba.util import force_bytes
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)

metrics = MetricsWrapper(environment.metrics, "replays.processor")
from sentry_sdk import capture_exception

ReplayEventDict = Mapping[Any, Any]
RetentionDays = int


USER_FIELDS_PRECEDENCE = ("user_id", "username", "email", "ip_address")


class ReplaysProcessor(MessageProcessor):
    def __extract_timestamp(self, field: int) -> datetime:
        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(field))
        if timestamp is None:
            timestamp = datetime.utcnow()
        return timestamp

    def _get_url(self, replay_event: ReplayEventDict) -> Optional[str]:
        if "request" in replay_event:
            if "url" in replay_event["request"]:
                return cast(str, replay_event["request"]["url"])
        return None

    def _process_base_replay_event_values(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        processed["replay_id"] = str(uuid.UUID(replay_event["replay_id"]))
        processed["segment_id"] = replay_event["segment_id"]
        processed["trace_ids"] = replay_event.get("trace_ids") or []
        processed["timestamp"] = self.__extract_timestamp(
            replay_event["timestamp"],
        )
        processed["release"] = replay_event.get("release")
        processed["environment"] = replay_event.get("environment")
        processed["dist"] = replay_event.get("dist")
        # processed["url"] = self._get_url(replay_event) TODO: add this in once we have the url column
        processed["platform"] = _unicodify(replay_event["platform"])

    def _process_tags(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        tags: MutableMapping[str, Any] = _as_dict_safe(replay_event.get("tags", None))
        if "transaction" in tags:
            processed["title"] = tags["transaction"]
        tags.pop("transaction", None)
        processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)

    def _add_user_column(
        self,
        processed: MutableMapping[str, Any],
        user_data: MutableMapping[str, Any],
    ) -> None:
        # TODO: this could be optimized / removed

        for field in USER_FIELDS_PRECEDENCE:
            if field in user_data and user_data[field]:
                processed["user"] = user_data[field]
                return

    def _process_user(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        user_dict = replay_event.get("user") or {}
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

        self._add_user_column(processed, user_data)

    def _process_sdk(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        sdk = replay_event.get("sdk", None) or {}
        processed["sdk_name"] = _unicodify(sdk.get("name") or "")
        processed["sdk_version"] = _unicodify(sdk.get("version") or "")

    def _process_kafka_metadata(
        self, metadata: KafkaMessageMetadata, processed: MutableMapping[str, Any]
    ) -> None:
        processed["partition"] = metadata.partition
        processed["offset"] = metadata.offset

    def _process_event_hash(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        # event_hash is used to uniquely identify a row within a replay
        # for our segment updates we'll simply hash the segment_id
        # for future additions, we'll use whatever unique identifier (e.g. event_id)
        # as the input to the hash

        segment_id_bytes = force_bytes(str((replay_event["segment_id"])))
        segment_hash = md5(segment_id_bytes).hexdigest()
        processed["event_hash"] = str(uuid.UUID(segment_hash))

    def process_message(
        self, message: Mapping[Any, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        try:
            replay_event = message["payload"]
            try:
                retention_days = enforce_retention(
                    message["retention_days"],
                    datetime.utcfromtimestamp(message["start_time"]),
                )
            except EventTooOld:
                return None

            processed: MutableMapping[str, Any] = {
                "retention_days": retention_days,
                "project_id": message["project_id"],
            }

            # # The following helper functions should be able to be applied in any order.
            # # At time of writing, there are no reads of the values in the `processed`
            # # dictionary to inform values in other functions.
            # # Ideally we keep continue that rule
            self._process_base_replay_event_values(processed, replay_event)
            self._process_tags(processed, replay_event)
            self._process_sdk(processed, replay_event)
            self._process_kafka_metadata(metadata, processed)

            # # the following operation modifies the event_dict and is therefore *not* order-independent
            self._process_user(processed, replay_event)
            self._process_event_hash(processed, replay_event)
            return InsertBatch([processed], None)
        except Exception as e:
            metrics.increment("consumer_error")
            capture_exception(e)
            return None
