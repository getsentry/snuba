import logging
import zlib
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Optional

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention
from snuba.processor import (
    InsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _ensure_valid_date,
)

DISABLED_MATERIALIZATION_VERSION = 1
ILLEGAL_VALUE_IN_SET = "Illegal value in set."
ILLEGAL_VALUE_FOR_COUNTER = "Illegal value for counter value."
INT_FLOAT_EXPECTED = "Int/Float expected"
INT_EXPECTED = "Int expected"

logger = logging.getLogger(__name__)

# {"org_id":1,"project_id":2,"use_case_id":"perf","name":"sentry.transactions.transaction.duration","unit":"ms","type":"s","value":[21,76,116,142,179],"timestamp":1655244629,"tags":{"6":91,"9":134,"4":159,"5":34}, "raw_tag_values":["prod","customer","flag_b","flag_a"], "metric_id":8,"retention_days":90}


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

        return zlib.adler32(
            bytearray(f"{use_case_id},{org_id},{project_id},{metric_id}", "utf-8")
        )

    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        # TODO: Support messages with multiple buckets
        if not self._should_process(message):
            return None

        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(message["timestamp"]))
        assert timestamp is not None, "Invalid timestamp"

        keys = []
        values = []
        tags = message["tags"]
        assert isinstance(tags, Mapping), "Invalid tags type"
        for key, value in sorted(tags.items()):
            assert key.isdigit() and isinstance(value, int), "Tag key/value invalid"
            keys.append(int(key))
            values.append(value)

        try:
            retention_days = enforce_retention(message["retention_days"], timestamp)
        except EventTooOld:
            return None

        raw_values = (
            ["" for _ in range(0, len(values))]
            if not message["raw_tag_values"]
            else message["raw_tag_values"]
        )

        assert len(raw_values) == len(
            values
        ), "Raw and indexed tag values must have the same length"

        logger.debug(f"timeseries_id = {self._hash_timeseries_id(message)}")
        processed = {
            "use_case_id": message["use_case_id"],
            "org_id": message["org_id"],
            "project_id": message["project_id"],
            "metric_id": message["metric_id"],
            "timestamp": timestamp,
            "tags.key": keys,
            "tags.raw_value": raw_values,
            "tags.indexed_value": values,
            **self._process_values(message),
            "materialization_version": 1,
            "retention_days": retention_days,
            # "partition": metadata.partition,
            # "offset": metadata.offset,
            "timeseries_id": self._hash_timeseries_id(message),
        }
        return InsertBatch([processed], None)


class SetsMetricsProcessor(MetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "s"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for value in values:
            assert isinstance(
                value, int
            ), f"{ILLEGAL_VALUE_IN_SET} {INT_EXPECTED}: {value}"
        return {"metric_type": OutputType.SET.value, "set_values": values}


class OutputType(Enum):
    SET = "set"
    COUNTER = "counter"
    DIST = "distribution"
