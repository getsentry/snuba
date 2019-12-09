import random
import simplejson as json
import uuid

from typing import Any, Dict, Optional
from datetime import datetime

from snuba import settings
from snuba.consumer import KafkaMessageMetadata
from snuba.processor import (
    _as_dict_safe,
    MessageProcessor,
    ProcessorAction,
    ProcessedMessage,
    _ensure_valid_date,
)
from snuba.datasets.events_processor import extract_extra_tags


class QueryLogMessageProcessor(MessageProcessor):
    PROMOTED_TAGS = {
        "referrer",
        "trace_id",
        "dataset",
        "from_clause",
        "columns",
        "limit",
        "offset",
    }

    def process_message(
        self, message: Dict[str, Any], metadata: Optional[KafkaMessageMetadata] = None
    ) -> Optional[ProcessedMessage]:
        action_type = ProcessorAction.INSERT
        sampling_rate = settings.QUERY_LOG_DATASET_SAMPLING_RATE
        if random.random() > sampling_rate:
            return None

        snuba_query = json.dumps(message.get("request", {}))
        timestamp = _ensure_valid_date(
            datetime.fromtimestamp(message["timing"]["timestamp"])
        )
        if timestamp is None:
            timestamp = datetime.utcnow()

        processed = {
            "clickhouse_query": message.get("sql", ""),
            "snuba_query": snuba_query,
            "timestamp": timestamp,
            "duration_ms": message["timing"]["duration_ms"],
            "hits": int(1 / sampling_rate),
        }

        tags = message["stats"]
        promoted_tags = {col: tags[col] for col in self.PROMOTED_TAGS if col in tags}

        processed["trace_id"] = str(uuid.UUID(promoted_tags["trace_id"]))
        processed["referrer"] = promoted_tags["referrer"]
        processed["from_clause"] = promoted_tags["from_clause"]
        processed["dataset"] = promoted_tags["dataset"]

        processed["limit"] = (
            int(promoted_tags["limit"]) if promoted_tags["limit"] else None
        )
        processed["offset"] = (
            int(promoted_tags["offset"]) if promoted_tags["offset"] else None
        )

        processed["columns"] = promoted_tags["columns"]

        tags = _as_dict_safe(tags)
        extract_extra_tags(processed, tags)
        return ProcessedMessage(action=action_type, data=[processed])
