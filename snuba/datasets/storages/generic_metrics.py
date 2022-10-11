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
from snuba.datasets.message_filters import KafkaHeaderSelectFilter
from snuba.datasets.metrics_messages import InputType
from snuba.datasets.processors.generic_metrics_processor import (
    GenericDistributionsMetricsProcessor,
    GenericSetsMetricsProcessor,
)
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.processors.physical.table_rate_limit import TableRateLimit
from snuba.query.processors.physical.tuple_unaliaser import TupleUnaliaser
from snuba.subscriptions.utils import SchedulingWatermarkMode
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.streams.topics import Topic


def produce_policy_creator() -> DeadLetterQueuePolicy:
    """
    Produce all bad messages to dead-letter topic.
    """
    return ProduceInvalidMessagePolicy(
        KafkaProducer(
            build_kafka_producer_configuration(
                Topic.DEAD_LETTER_GSNUBA_DEAD_LETTER_GENERIC_METRICSENERIC_METRICS
            )
        ),
        KafkaTopic(Topic.SNUBA_DEAD_LETTER_GENERIC_METRICS.value),
    )


common_columns: Sequence[Column[SchemaModifiers]] = [
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
]

aggregate_common_columns: Sequence[Column[SchemaModifiers]] = [
    Column("_raw_tags_hash", Array(UInt(64), SchemaModifiers(readonly=True))),
    Column(
        "_indexed_tags_hash",
        Array(UInt(64), SchemaModifiers(readonly=True)),
    ),
    Column("granularity", UInt(8)),
]

aggregate_distributions_columns: Sequence[Column[SchemaModifiers]] = [
    Column(
        "percentiles",
        AggregateFunction("quantiles(0.5, 0.75, 0.9, 0.95, 0.99)", [Float(64)]),
    ),
    Column("min", AggregateFunction("min", [Float(64)])),
    Column("max", AggregateFunction("max", [Float(64)])),
    Column("avg", AggregateFunction("avg", [Float(64)])),
    Column("sum", AggregateFunction("sum", [Float(64)])),
    Column("count", AggregateFunction("count", [Float(64)])),
    Column(
        "histogram_buckets",
        AggregateFunction("histogram(250)", [Float(64)]),
    ),
]

bucket_columns: Sequence[Column[SchemaModifiers]] = [
    Column("granularities", Array(UInt(8))),
    Column("count_value", Float(64)),
    Column("set_values", Array(UInt(64))),
    Column("distribution_values", Array(Float(64))),
    Column("timeseries_id", UInt(32)),
]

shared_query_processors: Sequence[ClickhouseQueryProcessor] = [
    TableRateLimit(),
    TupleUnaliaser(),
]

sets_storage = ReadableTableStorage(
    storage_key=StorageKey.GENERIC_METRICS_SETS,
    storage_set_key=StorageSetKey.GENERIC_METRICS_SETS,
    schema=TableSchema(
        local_table_name="generic_metric_sets_local",
        dist_table_name="generic_metric_sets_aggregated_dist",
        storage_set_key=StorageSetKey.GENERIC_METRICS_SETS,
        columns=ColumnSet(
            [
                *common_columns,
                *aggregate_common_columns,
                Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
            ]
        ),
    ),
    query_processors=shared_query_processors,
)

sets_bucket_storage = WritableTableStorage(
    storage_key=StorageKey.GENERIC_METRICS_SETS_RAW,
    storage_set_key=StorageSetKey.GENERIC_METRICS_SETS,
    schema=WritableTableSchema(
        columns=ColumnSet([*common_columns, *bucket_columns]),
        local_table_name="generic_metric_sets_raw_local",
        dist_table_name="generic_metric_sets_raw_dist",
        storage_set_key=StorageSetKey.GENERIC_METRICS_SETS,
    ),
    query_processors=[],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=GenericSetsMetricsProcessor(),
        default_topic=Topic.SNUBA_GENERIC_METRICS,
        dead_letter_queue_policy_creator=produce_policy_creator,
        commit_log_topic=Topic.SNUBA_GENERIC_METRICS_SETS_COMMIT_LOG,
        subscription_scheduled_topic=Topic.SCHEDULED_SUBSCRIPTIONS_GENERIC_METRICS_SETS,
        subscription_scheduler_mode=SchedulingWatermarkMode.GLOBAL,
        subscription_result_topic=Topic.GENERIC_METRICS_SETS_SUBSCRIPTION_RESULTS,
        pre_filter=KafkaHeaderSelectFilter("metric_type", InputType.SET.value),
    ),
)

distributions_storage = ReadableTableStorage(
    storage_key=StorageKey.GENERIC_METRICS_DISTRIBUTIONS,
    storage_set_key=StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS,
    schema=TableSchema(
        local_table_name="generic_metric_distributions_aggregated_local",
        dist_table_name="generic_metric_distributions_aggregated_dist",
        storage_set_key=StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS,
        columns=ColumnSet(
            [
                *common_columns,
                *aggregate_common_columns,
                *aggregate_distributions_columns,
            ]
        ),
    ),
    query_processors=shared_query_processors,
)

distributions_bucket_storage = WritableTableStorage(
    storage_key=StorageKey.GENERIC_METRICS_DISTRIBUTIONS_RAW,
    storage_set_key=StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS,
    schema=WritableTableSchema(
        columns=ColumnSet([*common_columns, *bucket_columns]),
        local_table_name="generic_metric_distributions_raw_local",
        dist_table_name="generic_metric_distributions_raw_local",
        storage_set_key=StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS,
    ),
    query_processors=[],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=GenericDistributionsMetricsProcessor(),
        default_topic=Topic.SNUBA_GENERIC_METRICS,
        dead_letter_queue_policy_creator=produce_policy_creator,
        commit_log_topic=Topic.SNUBA_GENERIC_METRICS_DISTRIBUTIONS_COMMIT_LOG,
        subscription_scheduled_topic=Topic.SCHEDULED_SUBSCRIPTIONS_GENERIC_METRICS_DISTRIBUTIONS,
        subscription_scheduler_mode=SchedulingWatermarkMode.GLOBAL,
        subscription_result_topic=Topic.GENERIC_METRICS_DISTRIBUTIONS_SUBSCRIPTION_RESULTS,
        pre_filter=KafkaHeaderSelectFilter("metric_type", InputType.DISTRIBUTION.value),
    ),
)
