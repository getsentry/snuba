"""
The storages defined in this file are for the generic metrics system,
initially built to handle metrics-enhanced performance.
"""

import collections
from dataclasses import dataclass, fields
from typing import (
    Any,
    Callable,
    Mapping,
    MutableSequence,
    Optional,
    Sequence,
    Type,
    Union,
)

from arroyo import Topic as KafkaTopic
from arroyo.backends.kafka import KafkaProducer
from arroyo.processing.strategies.dead_letter_queue import (
    DeadLetterQueuePolicy,
    ProduceInvalidMessagePolicy,
)
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
from snuba.datasets.generic_metrics_processor import (
    GenericDistributionsMetricsProcessor,
    GenericSetsMetricsProcessor,
)
from snuba.datasets.message_filters import KafkaHeaderSelectFilter
from snuba.datasets.metrics_messages import InputType
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.deep_compare import deep_compare_storages

# from snuba.datasets.storages.deep_compare import deep_compare_storages
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


def dataclass_from_dict(cls: Type[Any], d: Any) -> Any:
    # recursively type cast dict to classes
    # HACK: super hacks
    try:
        if cls.__origin__ == collections.abc.Sequence:
            return [dataclass_from_dict(cls.__dict__["__args__"][0], cfg) for cfg in d]
        elif cls.__origin__ == Union:
            if isinstance(d[0], str) or isinstance(d[0], int):
                return d
            elif isinstance(d[0], dict):
                for t in cls.__dict__["__args__"]:
                    result = dataclass_from_dict(t, d)
                    if not isinstance(result[0], dict):
                        return result
            return d
    except Exception:
        pass

    if cls in {str, int}:
        return d

    # https://stackoverflow.com/a/54769644
    try:
        fieldtypes = {f.name: f.type for f in fields(cls)}
        return cls(**{f: dataclass_from_dict(fieldtypes[f], d[f]) for f in d})
    except Exception:
        return d


@dataclass(frozen=True)
class StorageMetadataConfig:
    key: str
    set_key: str


@dataclass(frozen=True)
class NestedColumnConfig:
    name: str
    type: str
    args: Sequence[int]


@dataclass(frozen=True)
class ColumnConfig:
    name: str
    type: str
    args: Union[Sequence[NestedColumnConfig], Sequence[Union[str, int]]]


@dataclass(frozen=True)
class SchemaConfig:
    # columns: Sequence[Union[Column[SchemaModifiers], tuple[str, ColumnType[SchemaModifiers]]]]
    # somehow configure this^
    columns: Sequence[ColumnConfig]
    local_table_name: str
    dist_table_name: str


@dataclass(frozen=True)
class FunctionCallConfig:
    type: str
    args: Sequence[str]


@dataclass(frozen=True)
class StreamLoaderConfig:
    processor: str
    default_topic: str
    commit_log_topic: str
    subscription_scheduled_topic: str
    subscription_scheduler_mode: str
    subscription_result_topic: str
    replacement_topic: Optional[str]
    pre_filter: FunctionCallConfig
    dlq_policy: FunctionCallConfig


@dataclass(frozen=True)
class StorageConfig:
    storage: StorageMetadataConfig
    schema: SchemaConfig
    stream_loader: StreamLoaderConfig


def policy_creator_creator(
    dlq_policy_conf: FunctionCallConfig,
) -> Optional[Callable[[], DeadLetterQueuePolicy]]:
    if dlq_policy_conf.type == "produce":
        dlq_topic = dlq_policy_conf.args[0]

        def produce_policy_creator2() -> DeadLetterQueuePolicy:
            return ProduceInvalidMessagePolicy(
                KafkaProducer(build_kafka_producer_configuration(Topic(dlq_topic))),
                KafkaTopic(dlq_topic),
            )

        return produce_policy_creator2
    return None


file = open("./snuba/datasets/configurations/generic_metrics.yaml")
conf_yml = safe_load(file)
assert isinstance(conf_yml, dict)
conf = dataclass_from_dict(StorageConfig, conf_yml)
assert isinstance(conf, StorageConfig)

# print(conf)

CONF_TO_PREFILTER: Mapping[str, Any] = {
    "kafka_header_select_filter": KafkaHeaderSelectFilter
}
CONF_TO_PROCESSOR: Mapping[str, Any] = {
    "generic_distributions_metrics_processor": GenericDistributionsMetricsProcessor
}


def parse_simple(
    col: Union[ColumnConfig, NestedColumnConfig]
) -> Column[SchemaModifiers]:
    if col.type == "UInt":
        assert isinstance(col.args[0], int)
        return Column(col.name, UInt(col.args[0]))
    elif col.type == "Float":
        assert isinstance(col.args[0], int)
        return Column(col.name, Float(col.args[0]))
    elif col.type == "String":
        return Column(col.name, String())
    elif col.type == "DateTime":
        return Column(col.name, DateTime())
    raise


def parse_columns(
    columns: Union[Sequence[ColumnConfig], Sequence[NestedColumnConfig]]
) -> Sequence[Column[SchemaModifiers]]:
    cols: MutableSequence[Column[SchemaModifiers]] = []

    SIMPLE_COLUMN_TYPES = {
        "UInt": UInt,
        "Float": Float,
        "String": String,
        "DateTime": DateTime,
    }

    for col in columns:
        column: Optional[Column[SchemaModifiers]] = None
        if col.type in SIMPLE_COLUMN_TYPES:
            column = parse_simple(col)
        elif col.type == "Nested":
            column = Column(col.name, Nested(parse_columns(col.args)))  # type: ignore
        elif col.type == "Array":
            subtype, value = col.args
            assert isinstance(subtype, str)
            assert isinstance(value, int)
            column = Column(col.name, Array(SIMPLE_COLUMN_TYPES[subtype](value)))
        assert column is not None
        cols.append(column)
    return cols


distributions_bucket_storage = WritableTableStorage(
    storage_key=StorageKey(conf.storage.key),
    storage_set_key=StorageSetKey(conf.storage.set_key),
    schema=WritableTableSchema(
        columns=ColumnSet(parse_columns(conf.schema.columns)),
        local_table_name=conf.schema.local_table_name,
        dist_table_name=conf.schema.dist_table_name,
        storage_set_key=StorageSetKey(conf.storage.set_key),
    ),
    query_processors=[],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=CONF_TO_PROCESSOR[conf.stream_loader.processor](),
        default_topic=Topic(conf.stream_loader.default_topic),
        dead_letter_queue_policy_creator=policy_creator_creator(
            conf.stream_loader.dlq_policy
        ),
        commit_log_topic=Topic(conf.stream_loader.commit_log_topic),
        subscription_scheduled_topic=Topic(
            conf.stream_loader.subscription_scheduled_topic
        ),
        subscription_scheduler_mode=SchedulingWatermarkMode(
            conf.stream_loader.subscription_scheduler_mode
        ),
        subscription_result_topic=Topic(conf.stream_loader.subscription_result_topic),
        replacement_topic=Topic(conf.stream_loader.replacement_topic)
        if conf.stream_loader.replacement_topic
        else None,
        pre_filter=CONF_TO_PREFILTER[conf.stream_loader.pre_filter.type](
            *conf.stream_loader.pre_filter.args
        ),
    ),
)


deep_compare_storages(distributions_bucket_storage_old, distributions_bucket_storage)
