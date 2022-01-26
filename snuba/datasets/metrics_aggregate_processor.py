from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Mapping, Optional

from snuba.clickhouse.value_types import (
    ClickhouseInt,
    ClickhouseNumArray,
    ClickhouseUnguardedExpression,
)
from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import (
    AggregateInsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _ensure_valid_date,
)


class MetricsAggregateProcessor(MessageProcessor, ABC):
    GRANULARITIES_SECONDS = [10, 3600, 3600 * 24]

    @abstractmethod
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError

    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
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
                "org_id": ClickhouseInt(message["org_id"]),
                "project_id": ClickhouseInt(message["project_id"]),
                "metric_id": ClickhouseInt(message["metric_id"]),
                "timestamp": ClickhouseUnguardedExpression(
                    f"toStartOfInterval(toDateTime('{timestamp.isoformat()}'), toIntervalSecond({granularity}))"
                ),
                "tags.key": ClickhouseNumArray(keys),
                "tags.value": ClickhouseNumArray(values),
                **self._process_values(message),
                "retention_days": ClickhouseInt(message["retention_days"]),
                "partition": metadata.partition,
                "offset": metadata.offset,
                "granularity": ClickhouseInt(granularity),
            }
            for granularity in self.GRANULARITIES_SECONDS
        ]
        return AggregateInsertBatch(processed, None)


class SetsAggregateProcessor(MetricsAggregateProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "s"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for v in values:
            assert isinstance(v, int), "Illegal value in set. Int expected: {v}"
        value_array = "[" + ",".join([str(v) for v in values]) + "]"

        return {
            "value": ClickhouseUnguardedExpression(
                f"arrayReduce('uniqState', {value_array})"
            )
        }


class CounterAggregateProcessor(MetricsAggregateProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "c"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        value = message["value"]
        assert isinstance(
            value, (int, float)
        ), "Illegal value for counter value. Int/Float expected {value}"

        return {
            "value": ClickhouseUnguardedExpression(
                f"arrayReduce('sumState', [{value}])"
            )
        }


class DistributionsAggregateProcessor(MetricsAggregateProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return message["type"] is not None and message["type"] == "d"

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for v in values:
            assert isinstance(
                v, (int, float)
            ), "Illegal value in set. Int/Float expected: {v}"

        value_array = "[" + ",".join([str(v) for v in values]) + "]"
        return {
            "percentiles": ClickhouseUnguardedExpression(
                f"arrayReduce('quantilesState(0.5,0.75,0.9,0.95,0.99)', {value_array})"
            ),
            "min": ClickhouseUnguardedExpression(
                f"arrayReduce('minState', {value_array})"
            ),
            "max": ClickhouseUnguardedExpression(
                f"arrayReduce('maxState', {value_array})"
            ),
            "avg": ClickhouseUnguardedExpression(
                f"arrayReduce('avgState', {value_array})"
            ),
            "sum": ClickhouseUnguardedExpression(
                f"arrayReduce('sumState', {value_array})"
            ),
            "count": ClickhouseUnguardedExpression(
                f"arrayReduce('countState', {value_array})"
            ),
        }
