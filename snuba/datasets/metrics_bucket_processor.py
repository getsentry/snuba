from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Optional

from arroyo.processing.strategies.dead_letter_queue import InvalidMessages

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
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
from snuba.utils.streams.topics import Topic

DISABLED_MATERIALIZATION_VERSION = 1


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

        processed = {
            "org_id": message["org_id"],
            "project_id": message["project_id"],
            "metric_id": message["metric_id"],
            "timestamp": timestamp,
            "tags.key": keys,
            "tags.value": values,
            **self._process_values(message),
            "materialization_version": mat_version,
            "retention_days": message["retention_days"],
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
                    message, f"Illegal value in set. Int expected: {value}"
                )
        return {"set_values": values}


class CounterMetricsProcessor(MetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "c"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        value = message["value"]
        if not isinstance(value, (int, float)):
            _raise_invalid_message(
                message, f"Illegal value for counter value. Int/Float expected: {value}"
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
                    message, f"Illegal value in set. Int/Float expected: {value}"
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
                        message, f"Illegal value in set. Int expected: {value}"
                    )
            return {"metric_type": OutputType.SET.value, "set_values": values}
        elif message["type"] == METRICS_COUNTERS_TYPE:
            value = message["value"]
            if not isinstance(value, (int, float)):
                _raise_invalid_message(
                    message,
                    f"Illegal value for counter value. Int/Float expected: {value}",
                )
            return {"metric_type": OutputType.COUNTER.value, "count_value": value}
        else:  # METRICS_DISTRIBUTIONS_TYPE
            values = message["value"]
            for value in values:
                if not isinstance(value, (int, float)):
                    _raise_invalid_message(
                        message, f"Illegal value in set. Int/Float expected: {value}"
                    )
            return {"metric_type": OutputType.DIST.value, "distribution_values": values}


def _raise_invalid_message(message: Mapping[str, Any], reason: str) -> None:
    """
    Pass an invalid message to the DLQ by raising `InvalidMessages` exception.
    """
    raise InvalidMessages(
        messages=[str(message)], reason=reason, original_topic=Topic.METRICS.value,
    )
