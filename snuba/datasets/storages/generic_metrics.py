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
from jsonschema import validate
from yaml import safe_load

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
from snuba.clickhouse.processors import QueryProcessor
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.configuration.config_loader import (
    CONF_TO_PREFILTER,
    CONF_TO_PROCESSOR,
    deep_compare_storages,
    get_query_processors,
    parse_columns,
    policy_creator_creator,
)
from snuba.datasets.configuration.generic_metrics.json_schema import (
    readable_storage_schema,
    writable_storage_schema,
)
from snuba.datasets.generic_metrics_processor import (
    GenericDistributionsMetricsProcessor,
    GenericSetsMetricsProcessor,
)
from snuba.datasets.message_filters import KafkaHeaderSelectFilter
from snuba.datasets.metrics_messages import InputType
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.table_rate_limit import TableRateLimit
from snuba.query.processors.tuple_unaliaser import TupleUnaliaser
from snuba.subscriptions.utils import SchedulingWatermarkMode
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

bucket_columns: Sequence[Column[SchemaModifiers]] = [
    Column("granularities", Array(UInt(8))),
    Column("count_value", Float(64)),
    Column("set_values", Array(UInt(64))),
    Column("distribution_values", Array(Float(64))),
    Column("timeseries_id", UInt(32)),
]

shared_query_processors: Sequence[QueryProcessor] = [TableRateLimit(), TupleUnaliaser()]

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
        default_topic=Topic.GENERIC_METRICS,
        dead_letter_queue_policy_creator=produce_policy_creator,
        commit_log_topic=Topic.GENERIC_METRICS_SETS_COMMIT_LOG,
        subscription_scheduled_topic=Topic.SUBSCRIPTION_SCHEDULED_GENERIC_METRICS_SETS,
        subscription_scheduler_mode=SchedulingWatermarkMode.GLOBAL,
        subscription_result_topic=Topic.SUBSCRIPTION_RESULTS_GENERIC_METRICS_SETS,
        pre_filter=KafkaHeaderSelectFilter("metric_type", InputType.SET.value),
    ),
)


config_file_path = "./snuba/datasets/configuration/generic_metrics"

dist_raw = open(f"{config_file_path}/storage_distributions_raw.yaml")
conf_dist_raw = safe_load(dist_raw)
validate(conf_dist_raw, writable_storage_schema)

dist_readonly = open(f"{config_file_path}/storage_distributions.yaml")
conf_dist_readonly = safe_load(dist_readonly)
validate(conf_dist_readonly, readable_storage_schema)

distributions_storage_old = ReadableTableStorage(
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
                Column(
                    "histogram_buckets",
                    AggregateFunction("histogram(250)", [Float(64)]),
                ),
            ]
        ),
    ),
    query_processors=shared_query_processors,
)

distributions_storage = ReadableTableStorage(
    storage_key=StorageKey(conf_dist_readonly["storage"]["key"]),
    storage_set_key=StorageSetKey(conf_dist_readonly["storage"]["set_key"]),
    schema=TableSchema(
        local_table_name="generic_metric_distributions_aggregated_local",
        dist_table_name="generic_metric_distributions_aggregated_dist",
        storage_set_key=StorageSetKey(conf_dist_readonly["storage"]["set_key"]),
        columns=ColumnSet(parse_columns(conf_dist_readonly["schema"]["columns"])),
    ),
    query_processors=get_query_processors(conf_dist_readonly["query_processors"]),
)

distributions_bucket_storage_old = WritableTableStorage(
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
        default_topic=Topic.GENERIC_METRICS,
        dead_letter_queue_policy_creator=produce_policy_creator,
        commit_log_topic=Topic.GENERIC_METRICS_DISTRIBUTIONS_COMMIT_LOG,
        subscription_scheduled_topic=Topic.SUBSCRIPTION_SCHEDULED_GENERIC_METRICS_DISTRIBUTIONS,
        subscription_scheduler_mode=SchedulingWatermarkMode.GLOBAL,
        subscription_result_topic=Topic.SUBSCRIPTION_RESULTS_GENERIC_METRICS_DISTRIBUTIONS,
        pre_filter=KafkaHeaderSelectFilter("metric_type", InputType.DISTRIBUTION.value),
    ),
)

distributions_bucket_storage = WritableTableStorage(
    storage_key=StorageKey(conf_dist_raw["storage"]["key"]),
    storage_set_key=StorageSetKey(conf_dist_raw["storage"]["set_key"]),
    schema=WritableTableSchema(
        columns=ColumnSet(parse_columns(conf_dist_raw["schema"]["columns"])),
        local_table_name=conf_dist_raw["schema"]["local_table_name"],
        dist_table_name=conf_dist_raw["schema"]["dist_table_name"],
        storage_set_key=StorageSetKey(conf_dist_raw["storage"]["set_key"]),
    ),
    query_processors=conf_dist_raw["query_processors"],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=CONF_TO_PROCESSOR[conf_dist_raw["stream_loader"]["processor"]](),
        default_topic=Topic(conf_dist_raw["stream_loader"]["default_topic"]),
        dead_letter_queue_policy_creator=policy_creator_creator(
            conf_dist_raw["stream_loader"]["dlq_policy"]
        ),
        commit_log_topic=Topic(conf_dist_raw["stream_loader"]["commit_log_topic"]),
        subscription_scheduled_topic=Topic(
            conf_dist_raw["stream_loader"]["subscription_scheduled_topic"]
        ),
        subscription_scheduler_mode=SchedulingWatermarkMode(
            conf_dist_raw["stream_loader"]["subscription_scheduler_mode"]
        ),
        subscription_result_topic=Topic(
            conf_dist_raw["stream_loader"]["subscription_result_topic"]
        ),
        replacement_topic=Topic(conf_dist_raw["stream_loader"]["replacement_topic"])
        if conf_dist_raw["stream_loader"]["replacement_topic"]
        else None,
        pre_filter=CONF_TO_PREFILTER[
            conf_dist_raw["stream_loader"]["pre_filter"]["type"]
        ](*conf_dist_raw["stream_loader"]["pre_filter"]["args"]),
    ),
)


deep_compare_storages(distributions_bucket_storage_old, distributions_bucket_storage)
deep_compare_storages(distributions_storage_old, distributions_storage)
