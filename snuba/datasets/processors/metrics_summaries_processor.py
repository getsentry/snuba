import time
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Mapping, MutableMapping, MutableSequence, Optional, Tuple

import structlog

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import (
    EventTooOld,
    enforce_retention,
    extract_extra_tags,
)
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import (
    InsertBatch,
    ProcessedMessage,
    _as_dict_safe,
    _ensure_valid_date,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = structlog.get_logger(__name__)

metrics = MetricsWrapper(environment.metrics, "metrics_summaries.processor")

SpanEvent = MutableMapping[str, Any]
MetricsSummaries = MutableSequence[MutableMapping[str, Any]]
RetentionDays = int


class MetricsSummariesMessageProcessor(DatasetMessageProcessor):
    """
    Message processor for writing metrics summary data to the metrics_summaries table.
    """

    def __extract_timestamp(self, timestamp_sec: float) -> int:
        # We are purposely using a naive datetime here to work with the rest of the codebase.
        # We can be confident that clients are only sending UTC dates.
        if _ensure_valid_date(datetime.utcfromtimestamp(timestamp_sec)) is None:
            timestamp_sec = int(time.time())
        return int(timestamp_sec)

    @staticmethod
    def _structure_and_validate_message(
        message: SpanEvent,
    ) -> Optional[Tuple[SpanEvent, RetentionDays]]:
        try:
            # We are purposely using a naive datetime here to work with the
            # rest of the codebase. We can be confident that clients are only
            # sending UTC dates.
            retention_days = enforce_retention(
                message.get("retention_days"),
                datetime.utcfromtimestamp(message["start_timestamp"]),
            )
        except EventTooOld:
            return None

        return message, retention_days

    def _process_metrics_summary_event(
        self,
        span_event: SpanEvent,
        retention_days: Optional[RetentionDays],
    ) -> MetricsSummaries:
        common_fields = {
            "deleted": 0,
            "end_timestamp": self.__extract_timestamp(span_event["end_timestamp"]),
            "project_id": span_event["project_id"],
            "retention_days": retention_days,
            "span_id": int(span_event["span_id"], 16),
            "trace_id": str(uuid.UUID(span_event["trace_id"])),
        }

        tags: Mapping[str, Any] = _as_dict_safe(span_event.get("tags", None))
        common_fields["tags.key"], common_fields["tags.value"] = extract_extra_tags(
            tags
        )

        processed_rows: MetricsSummaries = []

        for metric_mri, metric_values in span_event.get("metrics_summary", {}).items():
            for metric_value in metric_values:
                processed = deepcopy(common_fields)

                processed["metric_mri"] = metric_mri
                processed["count"] = int(metric_value["count"])

                for key in {"min", "max", "sum"}:
                    processed[key] = float(metric_value[key])

                processed_rows.append(processed)

        return processed_rows

    def process_message(
        self,
        message: SpanEvent,
        metadata: KafkaMessageMetadata,
    ) -> Optional[ProcessedMessage]:
        span_event, retention_days = self._structure_and_validate_message(message) or (
            None,
            None,
        )
        if not span_event:
            return None
        try:
            processed_rows = self._process_metrics_summary_event(
                span_event, retention_days
            )
        except Exception:
            metrics.increment("message_processing_error")
            return None

        received = (
            datetime.fromtimestamp(span_event["received"], tz=timezone.utc)
            if "received" in span_event
            else None
        )
        return InsertBatch(rows=processed_rows, origin_timestamp=received)
