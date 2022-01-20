from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Mapping, Optional

from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import (
    InsertBatch,
    InsertBatchInterpretExpressions,
    MessageProcessor,
    ProcessedMessage,
    _ensure_valid_date,
)


class MetricsProcessor(MessageProcessor, ABC):
    @abstractmethod
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError

    def _contains_interpretable_values(self) -> bool:
        return False

    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        # TODO: Support messages with multiple buckets

        if not self._should_process(message):
            return None

        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(message["timestamp"]))
        assert timestamp is not None

        keys = []
        values = []
        tags = message["tags"]
        assert isinstance(tags, Mapping)
        for key, value in sorted(tags.items()):
            assert key.isdigit()
            keys.append(int(key))
            assert isinstance(value, int)
            values.append(value)

        processed = {
            "org_id": message["org_id"],
            "project_id": message["project_id"],
            "metric_id": message["metric_id"],
            "timestamp": timestamp,
            "tags.key": keys,
            "tags.value": values,
            **self._process_values(message),
            "materialization_version": 0,
            "retention_days": message["retention_days"],
            "partition": metadata.partition,
            "offset": metadata.offset,
        }

        if self._contains_interpretable_values():
            return InsertBatchInterpretExpressions([processed])
        else:
            return InsertBatch([processed], None)


class SetsMetricsProcessor(MetricsProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "s"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for v in values:
            assert isinstance(v, int), "Illegal value in set. Int expected: {v}"
        return {"set_values": values}


class CounterMetricsProcessor(MetricsProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "c"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        value = message["value"]
        assert isinstance(
            value, (int, float)
        ), "Illegal value for counter value. Int/Float expected {value}"
        return {"value": value}


class DistributionsMetricsProcessor(MetricsProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "d"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for v in values:
            assert isinstance(
                v, (int, float)
            ), "Illegal value in set. Int expected: {v}"
        return {"values": values}


class DistributionsMetricsProcessorDirectWriter(MetricsProcessor):
    def _contains_interpretable_values(self) -> bool:
        return True

    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "d"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for v in values:
            assert isinstance(
                v, (int, float)
            ), "Illegal value in set. Int expected: {v}"
        escaped_array = "[" + ",".join([str(v) for v in values]) + "]"
        return {
            "percentiles": f"arrayReduce('quantilesState(0.5,0.75,0.9,0.95,0.99)', {escaped_array})"
        }
