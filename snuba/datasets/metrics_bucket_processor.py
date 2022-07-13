import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Mapping, Optional

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention
from snuba.datasets.metrics_messages import (
    ILLEGAL_VALUE_IN_DIST,
    ILLEGAL_VALUE_IN_SET,
    INT_EXPECTED,
    INT_FLOAT_EXPECTED,
    InputType,
    OutputType,
    is_set_message,
    values_for_distribution_message,
    values_for_set_message,
)
from snuba.processor import (
    InsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _ensure_valid_date,
)

DISABLED_MATERIALIZATION_VERSION = 1
ILLEGAL_VALUE_FOR_COUNTER = "Illegal value for counter value."

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
        assert timestamp is not None, "Invalid timestamp"

        keys = []
        values = []
        tags = message["tags"]
        assert isinstance(tags, Mapping), "Invalid tags type"
        for key, value in sorted(tags.items()):
            assert key.isdigit() and isinstance(value, int), "Tag key/value invalid"
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
        return is_set_message(message)

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for value in values:
            assert isinstance(
                value, int
            ), f"{ILLEGAL_VALUE_IN_SET} {INT_EXPECTED}: {value}"
        return {"set_values": values}


class CounterMetricsProcessor(MetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "c"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        value = message["value"]
        assert isinstance(
            value, (int, float)
        ), f"{ILLEGAL_VALUE_FOR_COUNTER} {INT_FLOAT_EXPECTED}: {value}"
        return {"value": value}


class DistributionsMetricsProcessor(MetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "d"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for value in values:
            assert isinstance(
                value, (int, float)
            ), f"{ILLEGAL_VALUE_IN_DIST} {INT_FLOAT_EXPECTED}: {value}"
        return {"values": values}


class PolymorphicMetricsProcessor(MetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] in {
            InputType.SET.value,
            InputType.COUNTER.value,
            InputType.DISTRIBUTION.value,
        }

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        if message["type"] == InputType.SET.value:
            return values_for_set_message(message)
        elif message["type"] == InputType.COUNTER.value:
            value = message["value"]
            assert isinstance(
                value, (int, float)
            ), f"{ILLEGAL_VALUE_FOR_COUNTER} {INT_FLOAT_EXPECTED}: {value}"
            return {"metric_type": OutputType.COUNTER.value, "count_value": value}
        else:  # message["type"] == InputType.DISTRIBUTION.value
            return values_for_distribution_message(message)
