"""
The storages defined in this file are for release-health metrics.
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
from snuba.datasets.metrics_aggregate_processor import (
    CounterAggregateProcessor,
    DistributionsAggregateProcessor,
    SetsAggregateProcessor,
)
from snuba.datasets.metrics_bucket_processor import PolymorphicMetricsProcessor
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema, WriteFormat
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.physical.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.table_rate_limit import TableRateLimit
from snuba.subscriptions.utils import SchedulingWatermarkMode
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic

PRE_VALUE_COLUMNS: Sequence[Column[SchemaModifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("metric_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("tags", Nested([Column("key", UInt(64)), Column("value", UInt(64))])),
]

POST_VALUE_COLUMNS: Sequence[Column[SchemaModifiers]] = [
    Column("materialization_version", UInt(8)),
    Column("retention_days", UInt(16)),
    Column("partition", UInt(16)),
    Column("offset", UInt(64)),
]


def produce_policy_creator() -> DeadLetterQueuePolicy:
    """
    Produce all bad messages to dead-letter topic.
    """
    return ProduceInvalidMessagePolicy(
        KafkaProducer(build_kafka_producer_configuration(Topic.DEAD_LETTER_METRICS)),
        KafkaTopic(Topic.DEAD_LETTER_METRICS.value),
    )


polymorphic_bucket = WritableTableStorage(
    storage_key=StorageKey.METRICS_RAW,
    storage_set_key=StorageSetKey.METRICS,
    schema=WritableTableSchema(
        columns=ColumnSet(
            [
                Column("use_case_id", String()),
                *PRE_VALUE_COLUMNS,
                Column("count_value", Float(64)),
                Column("set_values", Array(UInt(64))),
                Column("distribution_values", Array(Float(64))),
                *POST_VALUE_COLUMNS,
            ]
        ),
        local_table_name="metrics_raw_v2_local",
        dist_table_name="metrics_raw_v2_dist",
        storage_set_key=StorageSetKey.METRICS,
    ),
    query_processors=[],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=PolymorphicMetricsProcessor(),
        default_topic=Topic.METRICS,
        commit_log_topic=Topic.METRICS_COMMIT_LOG,
        subscription_scheduler_mode=SchedulingWatermarkMode.GLOBAL,
        subscription_scheduled_topic=Topic.SUBSCRIPTION_SCHEDULED_METRICS,
        subscription_result_topic=Topic.SUBSCRIPTION_RESULTS_METRICS,
        dead_letter_queue_policy_creator=produce_policy_creator,
    ),
)

aggregated_columns = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("metric_id", UInt(64)),
    Column("granularity", UInt(32)),
    Column("timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column("tags", Nested([("key", UInt(64)), ("value", UInt(64))])),
    Column("_tags_hash", Array(UInt(64), SchemaModifiers(readonly=True))),
]


sets_storage = WritableTableStorage(
    storage_key=StorageKey.METRICS_SETS,
    storage_set_key=StorageSetKey.METRICS,
    schema=WritableTableSchema(
        local_table_name="metrics_sets_v2_local",
        dist_table_name="metrics_sets_v2_dist",
        storage_set_key=StorageSetKey.METRICS,
        columns=ColumnSet(
            [
                *aggregated_columns,
                Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
            ]
        ),
    ),
    query_processors=[ArrayJoinKeyValueOptimizer("tags"), TableRateLimit()],
    stream_loader=build_kafka_stream_loader_from_settings(
        SetsAggregateProcessor(),
        default_topic=Topic.METRICS,
        dead_letter_queue_policy_creator=produce_policy_creator,
    ),
    write_format=WriteFormat.VALUES,
)

counters_storage = WritableTableStorage(
    storage_key=StorageKey.METRICS_COUNTERS,
    storage_set_key=StorageSetKey.METRICS,
    schema=WritableTableSchema(
        local_table_name="metrics_counters_v2_local",
        dist_table_name="metrics_counters_v2_dist",
        storage_set_key=StorageSetKey.METRICS,
        columns=ColumnSet(
            [
                *aggregated_columns,
                Column("value", AggregateFunction("sum", [Float(64)])),
            ]
        ),
    ),
    query_processors=[ArrayJoinKeyValueOptimizer("tags"), TableRateLimit()],
    stream_loader=build_kafka_stream_loader_from_settings(
        CounterAggregateProcessor(),
        default_topic=Topic.METRICS,
        dead_letter_queue_policy_creator=produce_policy_creator,
    ),
    write_format=WriteFormat.VALUES,
)

org_counters_storage = ReadableTableStorage(
    storage_key=StorageKey.ORG_METRICS_COUNTERS,
    storage_set_key=StorageSetKey.METRICS,
    schema=TableSchema(
        local_table_name="metrics_counters_v2_local",
        dist_table_name="metrics_counters_v2_dist",
        storage_set_key=StorageSetKey.METRICS,
        columns=ColumnSet(
            [
                Column("org_id", UInt(64)),
                Column("project_id", UInt(64)),
                Column("metric_id", UInt(64)),
                Column("granularity", UInt(32)),
                Column("timestamp", DateTime()),
            ]
        ),
    ),
    query_processors=[TableRateLimit()],
)


distributions_storage = WritableTableStorage(
    storage_key=StorageKey.METRICS_DISTRIBUTIONS,
    storage_set_key=StorageSetKey.METRICS,
    schema=WritableTableSchema(
        local_table_name="metrics_distributions_v2_local",
        dist_table_name="metrics_distributions_v2_dist",
        storage_set_key=StorageSetKey.METRICS,
        columns=ColumnSet(
            [
                *aggregated_columns,
                Column(
                    "percentiles",
                    AggregateFunction(
                        "quantiles(0.5, 0.75, 0.9, 0.95, 0.99)", [Float(64)]
                    ),
                ),
                Column("min", AggregateFunction("min", [Float(64)])),
                Column("max", AggregateFunction("max", [Float(64)])),
                Column("avg", AggregateFunction("avg", [Float(64)])),
                Column("sum", AggregateFunction("sum", [Float(64)])),
                Column("count", AggregateFunction("count", [Float(64)])),
            ]
        ),
    ),
    query_processors=[ArrayJoinKeyValueOptimizer("tags"), TableRateLimit()],
    stream_loader=build_kafka_stream_loader_from_settings(
        DistributionsAggregateProcessor(),
        default_topic=Topic.METRICS,
        dead_letter_queue_policy_creator=produce_policy_creator,
    ),
    write_format=WriteFormat.VALUES,
)
