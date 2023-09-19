import logging
import pickle
import zlib
from abc import ABC, abstractmethod, abstractproperty
from datetime import datetime
from random import random
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

from sentry_kafka_schemas.schema_types.snuba_generic_metrics_v1 import GenericMetric
from usageaccountant import UsageUnit

from snuba.cogs.accountant import record_cogs
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention
from snuba.datasets.metrics_messages import (
    aggregation_options_for_counter_message,
    aggregation_options_for_distribution_message,
    aggregation_options_for_set_message,
    is_counter_message,
    is_distribution_message,
    is_set_message,
    value_for_counter_message,
    values_for_distribution_message,
    values_for_set_message,
)
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage, _ensure_valid_date
from snuba.state import get_config

logger = logging.getLogger(__name__)


class GenericMetricsBucketProcessor(DatasetMessageProcessor, ABC):
    @abstractmethod
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def _aggregation_options(
        self, message: Mapping[str, Any], retention_days: int
    ) -> Mapping[str, Any]:
        raise NotImplementedError

    @abstractproperty
    def _resource_id(self) -> str:
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
        for key, value in sorted_tag_items:
            buffer.extend(bytes(key, "utf-8"))
            if isinstance(value, int):
                buffer.extend(value.to_bytes(length=8, byteorder="little"))
            elif isinstance(value, str):
                buffer.extend(bytes(value, "utf-8"))

        return buffer

    def _hash_timeseries_id(
        self, message: GenericMetric, sorted_tag_items: Iterable[Tuple[str, str]]
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
        self, message: GenericMetric, metadata: KafkaMessageMetadata
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
            **self._aggregation_options(message, retention_days),
            "retention_days": retention_days,
            "timeseries_id": self._hash_timeseries_id(message, sorted_tag_items),
        }
        sentry_received_timestamp = None
        if message.get("sentry_received_timestamp"):
            sentry_received_timestamp = datetime.utcfromtimestamp(
                message["sentry_received_timestamp"]
            )

        self.__record_cogs(message)
        return InsertBatch(
            [processed], None, sentry_received_timestamp=sentry_received_timestamp
        )

    def __record_cogs(self, message: GenericMetric) -> None:
        if random() < (get_config("gen_metrics_processor_cogs_probability") or 0):
            record_cogs(
                resource_id=self._resource_id,
                app_feature=f"genericmetrics_{message['use_case_id']}",
                amount=len(pickle.dumps(message)),
                usage_type=UsageUnit.BYTES,
            )


class GenericSetsMetricsProcessor(GenericMetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return is_set_message(message)

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        return values_for_set_message(message)

    def _aggregation_options(
        self, message: Mapping[str, Any], retention_days: int
    ) -> Mapping[str, Any]:
        return aggregation_options_for_set_message(message, retention_days)

    @property
    def _resource_id(self) -> str:
        return "generic_metrics_processor_sets"


class GenericDistributionsMetricsProcessor(GenericMetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return is_distribution_message(message)

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        return values_for_distribution_message(message)

    def _aggregation_options(
        self, message: Mapping[str, Any], retention_days: int
    ) -> Mapping[str, Any]:
        return aggregation_options_for_distribution_message(message, retention_days)

    @property
    def _resource_id(self) -> str:
        return "generic_metrics_processor_distributions"


class GenericCountersMetricsProcessor(GenericMetricsBucketProcessor):
    def _should_process(self, message: Mapping[str, Any]) -> bool:
        return is_counter_message(message)

    def _process_values(self, message: Mapping[str, Any]) -> Mapping[str, Any]:
        return value_for_counter_message(message)

    def _aggregation_options(
        self, message: Mapping[str, Any], retention_days: int
    ) -> Mapping[str, Any]:
        return aggregation_options_for_counter_message(message, retention_days)

    @property
    def _resource_id(self) -> str:
        return "generic_metrics_processor_counters"
