import zlib
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Tuple, Union

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention
from snuba.datasets.metrics_messages import (
    INT_FLOAT_EXPECTED,
    InputType,
    OutputType,
    values_for_distribution_message,
    values_for_set_message,
)
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage, _ensure_valid_date

ENABLED_MATERIALIZATION_VERSION = 4

ILLEGAL_VALUE_FOR_COUNTER = "Illegal value for counter value."


class MetricsBucketProcessor(DatasetMessageProcessor, ABC):
    @abstractmethod
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError

    def _timeseries_id_token(
        self,
        message: Mapping[str, Any],
        sorted_tag_items: Iterable[Tuple[str, Union[int, str]]],
    ) -> bytearray:
        org_id: int = message["org_id"]
        project_id: int = message["project_id"]
        metric_id: int = message["metric_id"]

        buffer = bytearray()
        for field in [org_id, project_id, metric_id]:
            buffer.extend(field.to_bytes(length=8, byteorder="little"))
        for (key, value) in sorted_tag_items:
            buffer.extend(bytes(key, "utf-8"))
            if isinstance(value, int):
                buffer.extend(value.to_bytes(length=8, byteorder="little"))
            elif isinstance(value, str):
                buffer.extend(bytes(value, "utf-8"))

        return buffer

    def _hash_timeseries_id(
        self, message: Mapping[str, Any], sorted_tag_items: Iterable[Tuple[str, int]]
    ) -> int:
        """
        _hash_timeseries_id should return a UInt32 whose distribution should shard
        as evenly as possible while ensuring that an average query will not have to
        cross shards to read a results (so for the same org, project, metric, and tags
        ClickHouse should not have to aggregate results from multiple nodes).
        """
        token = self._timeseries_id_token(message, sorted_tag_items)
        return zlib.adler32(token)

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

        sorted_tag_items = sorted(tags.items())
        for key, value in sorted_tag_items:
            assert key.isdigit() and isinstance(value, int), "Tag key/value invalid"
            keys.append(int(key))
            values.append(value)

        mat_version = ENABLED_MATERIALIZATION_VERSION

        try:
            retention_days = enforce_retention(message["retention_days"], timestamp)
        except EventTooOld:
            return None

        processed = {
            "org_id": message["org_id"],
            "project_id": message["project_id"],
            "metric_id": message["metric_id"],
            "use_case_id": message.get("use_case_id", "sessions"),
            "timestamp": timestamp,
            "tags.key": keys,
            "tags.value": values,
            **self._process_values(message),
            "materialization_version": mat_version,
            "retention_days": retention_days,
            "timeseries_id": self._hash_timeseries_id(message, sorted_tag_items),
            "partition": metadata.partition,
            "offset": metadata.offset,
        }
        sentry_received_timestamp = None
        if message.get("sentry_received_timestamp"):
            sentry_received_timestamp = datetime.utcfromtimestamp(
                message["sentry_received_timestamp"]
            )

        return InsertBatch(
            [processed], None, sentry_received_timestamp=sentry_received_timestamp
        )


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


def timestamp_to_bucket(timestamp: datetime, interval_seconds: int) -> datetime:
    time_seconds = timestamp.timestamp()
    out_seconds = interval_seconds * (time_seconds // interval_seconds)
    return datetime.fromtimestamp(out_seconds, timestamp.tzinfo)
