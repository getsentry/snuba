import uuid
from datetime import datetime
from typing import Any, Mapping, Optional

from sentry_relay import DataCategory

from snuba import environment, settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import (
    InsertBatch,
    ProcessedMessage,
    _ensure_valid_date,
    _unicodify,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "outcomes.processor")


OUTCOME_ABUSE = 4
OUTCOME_CLIENT_DISCARD = 5

CLIENT_DISCARD_REASONS = frozenset(
    [
        "queue_overflow",
        "cache_overflow",
        "ratelimit_backoff",
        "network_error",
        "before_send",
        "event_processor",
        "sample_rate",
    ]
)


class OutcomesProcessor(DatasetMessageProcessor):
    def process_message(
        self, value: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        assert isinstance(value, dict)
        v_uuid = value.get("event_id")
        reason = value.get("reason")

        # relays let arbitrary outcome reasons through do the topic.  We
        # reject undesired values only in the processor so that we can
        # add new ones without having to update relays through the entire
        # chain.
        if value["outcome"] == OUTCOME_CLIENT_DISCARD:
            if reason is not None and reason not in CLIENT_DISCARD_REASONS:
                reason = None

        if (
            value["outcome"] != OUTCOME_ABUSE
        ):  # we dont care about abuse outcomes for these metrics
            if "category" not in value:
                metrics.increment("missing_category")
            if "quantity" not in value:
                metrics.increment("missing_quantity")

        message = None
        try:
            timestamp = _ensure_valid_date(
                datetime.strptime(value["timestamp"], settings.PAYLOAD_DATETIME_FORMAT),
            )
        except Exception:
            metrics.increment("bad_outcome_timestamp")
            timestamp = _ensure_valid_date(datetime.utcnow())

        try:
            message = {
                "org_id": value.get("org_id", 0),
                "project_id": value.get("project_id", 0),
                "key_id": value.get("key_id"),
                "timestamp": timestamp,
                "outcome": value["outcome"],
                "category": value.get("category", DataCategory.ERROR),
                "quantity": value.get("quantity", 1),
                "reason": _unicodify(reason),
                "event_id": str(uuid.UUID(v_uuid)) if v_uuid is not None else None,
            }
        except Exception:
            metrics.increment("bad_outcome")
            return None

        return InsertBatch([message], None)
