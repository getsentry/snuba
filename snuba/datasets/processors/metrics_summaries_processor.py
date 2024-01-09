import time
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Mapping, MutableMapping, MutableSequence, Optional, Tuple

import structlog
from sentry_kafka_schemas.schema_types.snuba_spans_v1 import SpanEvent

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import (
    EventTooOld,
    enforce_retention,
    extract_extra_tags,
)
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.datasets.processors.spans_processor import RetentionDays
from snuba.processor import (
    InsertBatch,
    ProcessedMessage,
    _as_dict_safe,
    _ensure_valid_date,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = structlog.get_logger(__name__)

metrics = MetricsWrapper(environment.metrics, "metrics_summaries.processor")

MetricsSummaries = MutableSequence[MutableMapping[str, Any]]


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
        message: SpanEvent,
    ) -> Optional[Tuple[SpanEvent, RetentionDays]]:
        if not message.get("_metrics_summary"):
            return None
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

    def _process_span_event(
        self,
        span_event: SpanEvent,
        retention_days: Optional[RetentionDays],
    ) -> MetricsSummaries:
        end_timestamp, _ = self.__extract_timestamp(
            span_event["start_timestamp_ms"] + span_event["duration_ms"],
        )
        common_fields = {
            "deleted": 0,
            "end_timestamp": end_timestamp,
            "project_id": span_event["project_id"],
            "retention_days": retention_days,
            "span_id": int(span_event["span_id"], 16),
            "trace_id": str(uuid.UUID(span_event["trace_id"])),
        }

        processed_rows: MetricsSummaries = []
        metrics_summary: Mapping[str, Any] = _as_dict_safe(
            span_event.get("_metrics_summary", None)
        )
        for metric_mri, metric_values in metrics_summary.items():
            for metric_value in metric_values:
                processed: MutableMapping[str, Any] = deepcopy(common_fields)

                tags: Mapping[str, Any] = _as_dict_safe(metric_value.get("tags", None))
                processed["tags.key"], processed["tags.value"] = extract_extra_tags(
                    tags
                )

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
            processed_rows = self._process_span_event(span_event, retention_days)
        except Exception:
            metrics.increment("message_processing_error")
            return None

        received = (
            datetime.fromtimestamp(span_event["received"], tz=timezone.utc)
            if "received" in span_event
            else None
        )
        return InsertBatch(rows=processed_rows, origin_timestamp=received)
