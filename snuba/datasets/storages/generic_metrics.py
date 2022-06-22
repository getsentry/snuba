"""
The storages defined in this file are for the generic metrics system,
initially built to handle metrics-enhanced performance.
"""
from typing import Sequence

from arroyo import Topic as KafkaTopic
from arroyo.backends.kafka import KafkaProducer
from arroyo.processing.strategies.dead_letter_queue import (
    DeadLetterQueuePolicy,
    ProduceInvalidMessagePolicy,
)

from snuba.clickhouse.columns import (
    AggregateFunction,
    Array,
    Column,
    ColumnSet,
    DateTime,
    Float,
    Nested,
    SchemaModifiers,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.generic_metrics_processor import GenericSetsMetricsProcessor
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.table_rate_limit import TableRateLimit
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic


def produce_policy_creator() -> DeadLetterQueuePolicy:
    """
    Produce all bad messages to dead-letter topic.
    """
    return ProduceInvalidMessagePolicy(
        KafkaProducer(
            build_kafka_producer_configuration(Topic.DEAD_LETTER_GENERIC_METRICS)
        ),
        KafkaTopic(Topic.DEAD_LETTER_GENERIC_METRICS.value),
    )


aggregated_columns: Sequence[Column[SchemaModifiers]] = [
    Column("org_id", UInt(64)),
    Column("use_case_id", String()),
    Column("project_id", UInt(64)),
    Column("metric_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column(
        "tags",
        Nested(
            [("key", UInt(64)), ("indexed_value", UInt(64)), ("raw_value", String())]
        ),
    ),
    Column("timeseries_id", UInt(32)),
]

sets_storage = ReadableTableStorage(
    storage_key=StorageKey.GENERIC_METRICS_SETS,
    storage_set_key=StorageSetKey.GENERIC_METRICS_SETS,
    schema=TableSchema(
        local_table_name="generic_metrics_sets_local",
        dist_table_name="generic_metrics_sets_dist",
        storage_set_key=StorageSetKey.GENERIC_METRICS_SETS,
        columns=ColumnSet(
            [
                *aggregated_columns,
                Column(
                    "_raw_tags_hash", Array(UInt(64), SchemaModifiers(readonly=True))
                ),
                Column(
                    "_indexed_tags_hash",
                    Array(UInt(64), SchemaModifiers(readonly=True)),
                ),
                Column("granularity", UInt(8)),
                Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
            ]
        ),
    ),
    query_processors=[
        ArrayJoinKeyValueOptimizer("tags"),
        TableRateLimit(),
    ],
)

sets_bucket_storage = WritableTableStorage(
    storage_key=StorageKey.GENERIC_METRICS_SETS_RAW,
    storage_set_key=StorageSetKey.GENERIC_METRICS_SETS,
    schema=WritableTableSchema(
        columns=ColumnSet(
            [
                *aggregated_columns,
                Column("granularities", Array(UInt(8))),
                Column("count_value", Float(64)),
                Column("set_values", Array(UInt(64))),
                Column("distribution_values", Array(Float(64))),
            ]
        ),
        local_table_name="generic_metric_sets_raw_local",
        dist_table_name="generic_metric_sets_raw_dist",
        storage_set_key=StorageSetKey.GENERIC_METRICS_SETS,
    ),
    query_processors=[],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=GenericSetsMetricsProcessor(),
        default_topic=Topic.GENERIC_METRICS,
        # commit_log_topic=Topic.METRICS_COMMIT_LOG,
        # subscription_scheduler_mode=SchedulingWatermarkMode.GLOBAL,
        # subscription_scheduled_topic=Topic.SUBSCRIPTION_SCHEDULED_METRICS,
        # subscription_result_topic=Topic.SUBSCRIPTION_RESULTS_METRICS,
        dead_letter_queue_policy_creator=produce_policy_creator,
    ),
)
