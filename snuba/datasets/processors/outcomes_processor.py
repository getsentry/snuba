import uuid
from datetime import datetime, timezone
from typing import Optional

from sentry_kafka_schemas.schema_types.outcomes_v1 import Outcome
from sentry_relay.consts import DataCategory

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
        "send_error",
        "internal_sdk_error",
        "insufficient_data",
        "backpressure",
    ]
)


class OutcomesProcessor(DatasetMessageProcessor):
    def process_message(
        self, outcome: Outcome, metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        assert isinstance(outcome, dict)
        v_uuid = outcome.get("event_id")
        reason = outcome.get("reason")

        # relays let arbitrary outcome reasons through do the topic.  We
        # reject undesired values only in the processor so that we can
        # add new ones without having to update relays through the entire
        # chain.
        if outcome["outcome"] == OUTCOME_CLIENT_DISCARD:
            if reason is not None and reason not in CLIENT_DISCARD_REASONS:
                reason = None

        if (
            outcome["outcome"] != OUTCOME_ABUSE
        ):  # we dont care about abuse outcomes for these metrics
            if "category" not in outcome:
                metrics.increment("missing_category")
            if "quantity" not in outcome:
                metrics.increment("missing_quantity")

        try:
            timestamp_str = outcome["timestamp"]
            # strip out nanoseconds from timestamp using string slicing before
            # parsing, because apparently relay produces this data today
            timestamp_str = timestamp_str[0:26] + timestamp_str[-1:]
            timestamp = datetime.strptime(
                timestamp_str, settings.PAYLOAD_DATETIME_FORMAT
            )
            timestamp = _ensure_valid_date(timestamp)
        except Exception:
            metrics.increment("bad_outcome_timestamp")
            timestamp = _ensure_valid_date(datetime.utcnow())

        assert timestamp is not None

        message = {
            "org_id": outcome.get("org_id", 0),
            "project_id": outcome.get("project_id", 0),
            "key_id": outcome.get("key_id"),
            "timestamp": int(timestamp.replace(tzinfo=timezone.utc).timestamp()),
            "outcome": outcome["outcome"],
            "category": outcome.get("category", DataCategory.ERROR.value),
            "quantity": outcome.get("quantity", 1),
            "reason": _unicodify(reason),
            "event_id": str(uuid.UUID(v_uuid)) if v_uuid is not None else None,
        }

        return InsertBatch([message], None)
