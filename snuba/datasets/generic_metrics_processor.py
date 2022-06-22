import logging
import zlib
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Mapping, MutableMapping, Optional

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention
from snuba.datasets.metrics_messages import is_set_message, values_for_set_message
from snuba.processor import (
    InsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _ensure_valid_date,
)

logger = logging.getLogger(__name__)

# test message:
# {"org_id": 1, "project_id": 2, "use_case_id": "perf", "name": "sentry.transactions.transaction.duration", "unit": "ms", "type": "s", "value": [21, 76, 116, 142, 179], "timestamp": 1655244629, "tags": {"6": 91, "9": 134, "4": 159, "5": 34}, "metric_id": 8, "retention_days": 90, "mapping_meta": {"h": {"8": "duration", "6": "tag1", "91": "value1", "9": "tag2", "134": "value2"}, "c": {"4": "error_type", "159": "exception", "5": "tag3"}, "d": {"34": "value3"}}}

# These are the hardcoded values from the materialized view
GRANULARITY_ONE_MINUTE = 1
GRANULARITY_ONE_HOUR = 2
GRANULARITY_ONE_DAY = 3


class MetricsBucketProcessor(MessageProcessor, ABC):
    @abstractmethod
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError

    def _hash_timeseries_id(self, message: Mapping[str, Any]) -> int:
        use_case_id: str = message.get("use_case_id") or ""
        org_id: str = message["org_id"]
        project_id: str = message["project_id"]
        metric_id: str = message["metric_id"]
        tag_keys_comma_sep: str = ",".join(message["tags"].keys())

        return zlib.adler32(
            bytearray(
                f"{use_case_id},{org_id},{project_id},{metric_id},{tag_keys_comma_sep}",
                "utf-8",
            )
        )

    def _get_raw_values_index(self, message: Mapping[str, Any]) -> Mapping[str, str]:
        acc: MutableMapping[str, str] = dict()
        for _, values in message["mapping_meta"].items():
            assert isinstance(values, Mapping), "Invalid mapping metadata"
            acc.update(values)

        return acc

    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        if not self._should_process(message):
            return None

        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(message["timestamp"]))
        assert timestamp is not None, "Invalid timestamp"

        keys = []
        indexed_values = []
        tags = message["tags"]
        assert isinstance(tags, Mapping), "Invalid tags type"

        raw_values_index = self._get_raw_values_index(message)

        for key, value in sorted(tags.items()):
            assert key.isdigit() and isinstance(value, int), "Tag key/value invalid"
            keys.append(int(key))
            indexed_values.append(value)

        try:
            retention_days = enforce_retention(message["retention_days"], timestamp)
        except EventTooOld:
            return None

        raw_values = [raw_values_index.get(str(v), "") for v in indexed_values]

        logger.debug(f"timeseries_id = {self._hash_timeseries_id(message)}")
        processed = {
            "use_case_id": message["use_case_id"],
            "org_id": message["org_id"],
            "project_id": message["project_id"],
            "metric_id": message["metric_id"],
            "timestamp": timestamp,
            "tags.key": keys,
            "tags.raw_value": raw_values,
            "tags.indexed_value": indexed_values,
            **self._process_values(message),
            "materialization_version": 1,
            "retention_days": retention_days,
            "timeseries_id": self._hash_timeseries_id(message),
            "granularities": [
                GRANULARITY_ONE_MINUTE,
                GRANULARITY_ONE_HOUR,
                GRANULARITY_ONE_DAY,
            ],
        }
        return InsertBatch([processed], None)


class SetsMetricsProcessor(MetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return is_set_message(message)

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        return values_for_set_message(message)
