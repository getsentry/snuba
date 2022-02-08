from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence, Tuple

from snuba import environment, settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import (
    AggregateInsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _ensure_valid_date,
)
from snuba.query.expressions import (
    Expression,
    FunctionCall,
    Literal,
    OptionalScalarType,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

snuba_metrics = MetricsWrapper(environment.metrics, "metrics.processor")


def _literal(value: OptionalScalarType) -> Literal:
    return Literal(None, value)


def _array_literal(values: Sequence[OptionalScalarType]) -> FunctionCall:
    return FunctionCall(None, "array", tuple(map(_literal, values)))


def _call(function_name: str, arguments: Tuple[Expression, ...]) -> FunctionCall:
    return FunctionCall(None, function_name, arguments)


METRICS_SET_TYPE = "s"
METRICS_DISTRIBUTIONS_TYPE = "d"
METRICS_COUNTERS_TYPE = "c"


class MetricsAggregateProcessor(MessageProcessor, ABC):
    TEN_SECONDS = 10
    ONE_MINUTE = 60
    ONE_HOUR = 3600
    ONE_DAY = 3600 * 24
    GRANULARITIES_SECONDS = [TEN_SECONDS, ONE_MINUTE, ONE_HOUR, ONE_DAY]

    @abstractmethod
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError

    def timestamp_to_bucket(
        self, timestamp: datetime, interval_seconds: int
    ) -> datetime:
        time_seconds = timestamp.timestamp()
        out_seconds = interval_seconds * (time_seconds // interval_seconds)
        return datetime.fromtimestamp(out_seconds)

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
                "org_id": _literal(message["org_id"]),
                "project_id": _literal(message["project_id"]),
                "metric_id": _literal(message["metric_id"]),
                "timestamp": _call(
                    "toDateTime",
                    (
                        _literal(
                            self.timestamp_to_bucket(timestamp, granularity).isoformat()
                        ),
                    ),
                ),
                "tags.key": _array_literal(keys),
                "tags.value": _array_literal(values),
                **self._process_values(message),
                "retention_days": _literal(message["retention_days"]),
                "granularity": _literal(granularity),
            }
            for granularity in self.GRANULARITIES_SECONDS
        ]
        return AggregateInsertBatch(processed, None)


class SetsAggregateProcessor(MetricsAggregateProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return (
            settings.WRITE_METRICS_AGG_DIRECTLY
            and message["type"] is not None
            and message["type"] == METRICS_SET_TYPE
        )

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for v in values:
            assert isinstance(v, int), "Illegal value in set. Int expected: {v}"
        snuba_metrics.increment("set.size", len(values))

        return {
            "value": _call(
                "arrayReduce", (_literal("uniqState"), _array_literal(values)),
            )
        }


class CounterAggregateProcessor(MetricsAggregateProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return (
            settings.WRITE_METRICS_AGG_DIRECTLY
            and message["type"] is not None
            and message["type"] == METRICS_COUNTERS_TYPE
        )

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        value = message["value"]
        assert isinstance(
            value, (int, float)
        ), "Illegal value for counter value. Int/Float expected {value}"

        return {
            "value": _call(
                "arrayReduce", (_literal("sumState"), _array_literal([value]),),
            ),
        }


class DistributionsAggregateProcessor(MetricsAggregateProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return (
            settings.WRITE_METRICS_AGG_DIRECTLY
            and message["type"] is not None
            and message["type"] == METRICS_DISTRIBUTIONS_TYPE
        )

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        values = message["value"]
        for v in values:
            assert isinstance(
                v, (int, float)
            ), "Illegal value in set. Int/Float expected: {v}"
        snuba_metrics.increment("distribution.size", len(values))

        return {
            "percentiles": _call(
                "arrayReduce",
                (
                    _literal("quantilesState(0.5,0.75,0.9,0.95,0.99)"),
                    _array_literal(values),
                ),
            ),
            "min": _call(
                "arrayReduce", (_literal("minState"), _array_literal([min(values)])),
            ),
            "max": _call(
                "arrayReduce", (_literal("maxState"), _array_literal([max(values)])),
            ),
            "avg": _call(
                "arrayReduce", (_literal("avgState"), _array_literal(values),),
            ),
            "sum": _call(
                "arrayReduce", (_literal("sumState"), _array_literal([sum(values)]),),
            ),
            "count": _call(
                "arrayReduce",
                (_literal("countState"), _array_literal([float(len(values))]),),
            ),
        }
