import logging
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Optional

from arroyo.processing.strategies.dead_letter_queue import (
    InvalidMessages,
    InvalidRawMessage,
)

from snuba import settings, state
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention
from snuba.datasets.metrics_aggregate_processor import (
    METRICS_COUNTERS_TYPE,
    METRICS_DISTRIBUTIONS_TYPE,
    METRICS_SET_TYPE,
)
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


class MetricsBucketProcessor(MessageProcessor, ABC):
    @abstractmethod
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError

    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        # TODO: Support messages with multiple buckets

        if not self._should_process(message):
            return None

        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(message["timestamp"]))
        if timestamp is None:
            _raise_invalid_message(message, "Invalid timestamp")

        keys = []
        values = []
        tags = message["tags"]
        if not isinstance(tags, Mapping):
            _raise_invalid_message(message, "Invalid tags type")
        for key, value in sorted(tags.items()):
            if not key.isdigit() or not isinstance(value, int):
                _raise_invalid_message(message, "Tag key/value invalid")
            keys.append(int(key))
            values.append(value)

        mat_version = (
            DISABLED_MATERIALIZATION_VERSION
            if settings.WRITE_METRICS_AGG_DIRECTLY
            else settings.ENABLED_MATERIALIZATION_VERSION
        )

        try:
            retention_days = enforce_retention(message["retention_days"], timestamp)
        except EventTooOld:
            return None

        processed = {
            "org_id": message["org_id"],
            "project_id": message["project_id"],
            "metric_id": message["metric_id"],
            "timestamp": timestamp,
            "tags.key": keys,
            "tags.value": values,
            **self._process_values(message),
            "materialization_version": mat_version,
            "retention_days": retention_days,
            "partition": metadata.partition,
            "offset": metadata.offset,
        }
        return InsertBatch([processed], None)


class SetsMetricsProcessor(MetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "s"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for value in values:
            if not isinstance(value, int):
                _raise_invalid_message(
                    message, f"{ILLEGAL_VALUE_IN_SET} {INT_EXPECTED}: {value}"
                )
        return {"set_values": values}


class CounterMetricsProcessor(MetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "c"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        value = message["value"]
        if not isinstance(value, (int, float)):
            _raise_invalid_message(
                message, f"{ILLEGAL_VALUE_FOR_COUNTER} {INT_FLOAT_EXPECTED}: {value}"
            )
        return {"value": value}


class DistributionsMetricsProcessor(MetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "d"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for value in values:
            if not isinstance(value, (int, float)):
                _raise_invalid_message(
                    message, f"{ILLEGAL_VALUE_IN_SET} {INT_FLOAT_EXPECTED}: {value}"
                )
        return {"values": values}


class OutputType(Enum):
    SET = "set"
    COUNTER = "counter"
    DIST = "distribution"


class PolymorphicMetricsProcessor(MetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] in {
            METRICS_SET_TYPE,
            METRICS_COUNTERS_TYPE,
            METRICS_DISTRIBUTIONS_TYPE,
        }

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        if message["type"] == METRICS_SET_TYPE:
            values = message["value"]
            for value in values:
                if not isinstance(value, int):
                    _raise_invalid_message(
                        message, f"{ILLEGAL_VALUE_IN_SET} {INT_EXPECTED}: {value}"
                    )
            return {"metric_type": OutputType.SET.value, "set_values": values}
        elif message["type"] == METRICS_COUNTERS_TYPE:
            value = message["value"]
            if not isinstance(value, (int, float)):
                _raise_invalid_message(
                    message,
                    f"{ILLEGAL_VALUE_FOR_COUNTER} {INT_FLOAT_EXPECTED}: {value}",
                )
            return {"metric_type": OutputType.COUNTER.value, "count_value": value}
        else:  # METRICS_DISTRIBUTIONS_TYPE
            values = message["value"]
            for value in values:
                if not isinstance(value, (int, float)):
                    _raise_invalid_message(
                        message, f"{ILLEGAL_VALUE_IN_SET} {INT_FLOAT_EXPECTED}: {value}"
                    )
            return {"metric_type": OutputType.DIST.value, "distribution_values": values}


def _raise_invalid_message(message: Mapping[str, Any], reason: str) -> None:
    """
    Pass an invalid message to the DLQ by raising `InvalidMessages` exception.
    """
    if state.get_config("enable_metrics_dlq", False):
        raise InvalidMessages(
            [
                InvalidRawMessage(
                    payload=str(message),
                    reason=reason,
                )
            ]
        )
    else:
        logger.error(
            "Ignored an invalid message on Metrics! (Did not go to DLQ)",
            exc_info=True,
            extra={"message": message, "reason": reason},
        )
