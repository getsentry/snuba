import logging
import zlib
from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    Any,
    Iterable,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Tuple,
    Union,
)

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention
from snuba.datasets.metrics_messages import (
    is_distribution_message,
    is_set_message,
    values_for_distribution_message,
    values_for_set_message,
)
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage, _ensure_valid_date

logger = logging.getLogger(__name__)

# These are the hardcoded values from the materialized view
GRANULARITY_ONE_MINUTE = 1
GRANULARITY_ONE_HOUR = 2
GRANULARITY_ONE_DAY = 3


class GenericMetricsBucketProcessor(DatasetMessageProcessor, ABC):
    @abstractmethod
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError

    #
    # This is mainly split out from _hash_timeseries_id for unit-testing purposes
    #
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

        try:
            retention_days = enforce_retention(message["retention_days"], timestamp)
        except EventTooOld:
            return None

        keys = []
        indexed_values: MutableSequence[int] = []
        raw_values: MutableSequence[str] = []
        tags = message["tags"]
        version = message.get("version", 1)
        assert isinstance(tags, Mapping), "Invalid tags type"
        raw_values_index = self._get_raw_values_index(message)

        sorted_tag_items = sorted(tags.items())
        for key, value in sorted_tag_items:
            assert key.isdigit(), "Tag key invalid"
            keys.append(int(key))

            if version == 1:
                assert isinstance(value, int), "Tag value invalid"
                indexed_values.append(value)
                raw_values.append(raw_values_index.get(str(value), ""))
            elif version == 2:
                assert isinstance(value, str), "Tag value invalid"
                indexed_values.append(0)
                raw_values.append(value)

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
            "timeseries_id": self._hash_timeseries_id(message, sorted_tag_items),
            "granularities": [
                GRANULARITY_ONE_MINUTE,
                GRANULARITY_ONE_HOUR,
                GRANULARITY_ONE_DAY,
            ],
        }
        return InsertBatch([processed], None)


class GenericSetsMetricsProcessor(GenericMetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return is_set_message(message)

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        return values_for_set_message(message)


class GenericDistributionsMetricsProcessor(GenericMetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return is_distribution_message(message)

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        return values_for_distribution_message(message)
