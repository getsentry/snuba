from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Mapping, Optional

from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import (
    AggregateInsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _ensure_valid_date,
)


class MetricsAggregateProcessor(MessageProcessor, ABC):
    GRANULARITIES = [10, 3600, 3600 * 24]

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

        processed = [
            {
                "org_id": message["org_id"],
                "project_id": message["project_id"],
                "metric_id": message["metric_id"],
                "timestamp": f"toStartOfInterval(toDateTime('{timestamp.isoformat()}'), toIntervalSecond({granularity}))",
                "tags.key": keys,
                "tags.value": values,
                **self._process_values(message),
                "retention_days": message["retention_days"],
                "partition": metadata.partition,
                "offset": metadata.offset,
                "granularity": granularity,
            }
            for granularity in self.GRANULARITIES
        ]
        return AggregateInsertBatch(processed, None)


class SetsAggregateProcessor(MetricsAggregateProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "s"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for v in values:
            assert isinstance(v, int), "Illegal value in set. Int expected: {v}"
        return {"set_values": values}


class CounterAggregateProcessor(MetricsAggregateProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "c"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        value = message["value"]
        assert isinstance(
            value, (int, float)
        ), "Illegal value for counter value. Int/Float expected {value}"
        return {"value": value}


class DistributionsAggregateProcessor(MetricsAggregateProcessor):
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
            "percentiles": f"arrayReduce('quantilesState(0.5,0.75,0.9,0.95,0.99)', {escaped_array})",
            "min": f"arrayReduce('minState', {escaped_array})",
            "max": f"arrayReduce('maxState', {escaped_array})",
            "avg": f"arrayReduce('avgState', {escaped_array})",
            "sum": f"arrayReduce('sumState', {escaped_array})",
            "count": f"arrayReduce('countState', {escaped_array})",
        }
