import time
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping, MutableMapping, MutableSequence, Optional, Tuple

import structlog

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import (
    EventTooOld,
    _as_dict_safe,
    enforce_retention,
    extract_extra_tags,
)
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage, _ensure_valid_date
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = structlog.get_logger(__name__)

metrics = MetricsWrapper(environment.metrics, "metrics_summaries.processor")

MetricsSummaryEvent = MutableMapping[str, Any]
RetentionDays = int


class MetricsSummariesMessageProcessor(DatasetMessageProcessor):
    """
    Message processor for writing metrics summary data to the metrics_summaries table.
    """

    def __extract_timestamp(self, timestamp_ms: int) -> Tuple[int, int]:
        # We are purposely using a naive datetime here to work with the rest of the codebase.
        # We can be confident that clients are only sending UTC dates.
        timestamp_sec = timestamp_ms / 1000
        if _ensure_valid_date(datetime.utcfromtimestamp(timestamp_sec)) is None:
            timestamp_sec = int(time.time())
        return int(timestamp_sec), int(timestamp_ms % 1000)

    @staticmethod
    def _structure_and_validate_message(
        message: MetricsSummaryEvent,
    ) -> Optional[Tuple[MetricsSummaryEvent, RetentionDays]]:
        try:
            # We are purposely using a naive datetime here to work with the
            # rest of the codebase. We can be confident that clients are only
            # sending UTC dates.
            retention_days = enforce_retention(
                message.get("retention_days"),
                datetime.utcfromtimestamp(message["start_timestamp_ms"] / 1000),
            )
        except EventTooOld:
            return None

        return message, retention_days

    def _process_metrics_summary_event(
        self,
        processed: MutableMapping[str, Any],
        metrics_summary_event: MetricsSummaryEvent,
    ) -> None:
        processed["trace_id"] = str(uuid.UUID(metrics_summary_event["trace_id"]))
        processed["span_id"] = int(metrics_summary_event["span_id"], 16)
        processed["segment_id"] = int(metrics_summary_event["segment_id"], 16)
        processed["project_id"] = metrics_summary_event["project_id"]

        processed["start_timestamp"], processed["start_ms"] = self.__extract_timestamp(
            metrics_summary_event["start_timestamp_ms"],
        )
        processed["exclusive_time"] = float(metrics_summary_event["exclusive_time_ms"])
        processed["op"] = metrics_summary_event["op"]
        processed["group"] = metrics_summary_event["group"]

        for key in {"min", "max", "sum", "count"}:
            processed[key] = float(metrics_summary_event[key])

        tags: Mapping[str, Any] = _as_dict_safe(metrics_summary_event.get("tags", None))
        processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)

    def process_message(
        self,
        message: MetricsSummaryEvent,
        metadata: KafkaMessageMetadata,
    ) -> Optional[ProcessedMessage]:
        metrics_summary_event, retention_days = self._structure_and_validate_message(
            message
        ) or (
            None,
            None,
        )
        if not metrics_summary_event:
            return None

        processed_rows: MutableSequence[MutableMapping[str, Any]] = []
        processed: MutableMapping[str, Any] = {
            "deleted": 0,
            "retention_days": retention_days,
            "partition": metadata.partition,
            "offset": metadata.offset,
        }

        try:
            self._process_metrics_summary_event(processed, metrics_summary_event)
            processed_rows.append(processed)
        except Exception:
            metrics.increment("message_processing_error")
            return None

        received = (
            datetime.fromtimestamp(metrics_summary_event["received"], tz=timezone.utc)
            if "received" in metrics_summary_event
            else None
        )
        return InsertBatch(rows=processed_rows, origin_timestamp=received)
