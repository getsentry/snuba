import random
import simplejson as json

from typing import Optional
from datetime import datetime

from snuba.processor import (
    _as_dict_safe,
    MessageProcessor,
    ProcessorAction,
    ProcessedMessage,
    _ensure_valid_date,
    _ensure_valid_ip,
    _unicodify,
)
from snuba.datasets.events_processor import (
    enforce_retention,
    extract_base,
    extract_extra_contexts,
    extract_extra_tags,
    extract_user,
)
from snuba import settings


class QueryLogMessageProcessor(MessageProcessor):
    def process_message(self, message, metadata=None) -> Optional[ProcessedMessage]:
        action_type = ProcessorAction.INSERT
        if not (isinstance(message, (list, tuple)) and len(message) >= 2):
            return None
        sampling_rate = settings.QUERY_LOG_DATASET_SAMPLING_RATE
        if random.random() < sampling_rate:
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
            "referrer": message["stats"]["referrer"],
            "hits": int(1 / sampling_rate),
            "transaction_id": message["stats"]["transaction_id"],
            "span_id": message["stats"]["span_id"],
            "dataset": message["stats"]["dataset"],
            "from_clause": message["stats"]["clickhouse_table"],
            "columns": message["stats"]["columns"],
            "limit": message["stats"]["limit"],
            "offset": message["stats"]["offset"],
            "projects": message["stats"]["projects"],
            "tags": message["stats"]["limit"],
        }

        extract_extra_tags(processed, message["stats"]["tags"])
        return ProcessedMessage(action=action_type, data=[processed])
