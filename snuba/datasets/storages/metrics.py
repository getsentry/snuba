from typing import Sequence

from snuba.clickhouse.columns import (
    AggregateFunction,
    Array,
    Column,
    ColumnSet,
    DateTime,
    Float,
    Nested,
    SchemaModifiers,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.metrics_processor import (
    CounterMetricsProcessor,
    DistributionsMetricsProcessor,
    SetsMetricsProcessor,
)
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
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

sets_buckets = WritableTableStorage(
    storage_key=StorageKey.METRICS_BUCKETS,
    storage_set_key=StorageSetKey.METRICS,
    schema=WritableTableSchema(
        columns=ColumnSet(
            [
                *PRE_VALUE_COLUMNS,
                Column("set_values", Array(UInt(64))),
                *POST_VALUE_COLUMNS,
            ]
        ),
        local_table_name="metrics_buckets_local",
        dist_table_name="metrics_buckets_dist",
        storage_set_key=StorageSetKey.METRICS,
    ),
    query_processors=[],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=SetsMetricsProcessor(), default_topic=Topic.METRICS,
    ),
)


counters_buckets = WritableTableStorage(
    storage_key=StorageKey.METRICS_COUNTERS_BUCKETS,
    storage_set_key=StorageSetKey.METRICS,
    schema=WritableTableSchema(
        columns=ColumnSet(
            [*PRE_VALUE_COLUMNS, Column("value", Float(64)), *POST_VALUE_COLUMNS]
        ),
        local_table_name="metrics_counters_buckets_local",
        dist_table_name="metrics_counters_buckets_dist",
        storage_set_key=StorageSetKey.METRICS,
    ),
    query_processors=[],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=CounterMetricsProcessor(), default_topic=Topic.METRICS,
    ),
)

distributions_buckets = WritableTableStorage(
    storage_key=StorageKey.METRICS_DISTRIBUTIONS_BUCKETS,
    storage_set_key=StorageSetKey.METRICS,
    schema=WritableTableSchema(
        columns=ColumnSet(
            [
                *PRE_VALUE_COLUMNS,
                Column("values", Array(Float(64))),
                *POST_VALUE_COLUMNS,
            ]
        ),
        local_table_name="metrics_distributions_buckets_local",
        dist_table_name="metrics_distributions_buckets_dist",
        storage_set_key=StorageSetKey.METRICS,
    ),
    query_processors=[],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=DistributionsMetricsProcessor(), default_topic=Topic.METRICS,
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


sets_storage = ReadableTableStorage(
    storage_key=StorageKey.METRICS_SETS,
    storage_set_key=StorageSetKey.METRICS,
    schema=TableSchema(
        local_table_name="metrics_sets_local",
        dist_table_name="metrics_sets_dist",
        storage_set_key=StorageSetKey.METRICS,
        columns=ColumnSet(
            [
                *aggregated_columns,
                Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
            ]
        ),
    ),
    query_processors=[ArrayJoinKeyValueOptimizer("tags")],
)

counters_storage = ReadableTableStorage(
    storage_key=StorageKey.METRICS_COUNTERS,
    storage_set_key=StorageSetKey.METRICS,
    schema=TableSchema(
        local_table_name="metrics_counters_local",
        dist_table_name="metrics_counters_dist",
        storage_set_key=StorageSetKey.METRICS,
        columns=ColumnSet(
            [
                *aggregated_columns,
                Column("value", AggregateFunction("sum", [Float(64)])),
            ]
        ),
    ),
    query_processors=[ArrayJoinKeyValueOptimizer("tags")],
)


distributions_storage = ReadableTableStorage(
    storage_key=StorageKey.METRICS_DISTRIBUTIONS,
    storage_set_key=StorageSetKey.METRICS,
    schema=TableSchema(
        local_table_name="metrics_distributions_local",
        dist_table_name="metrics_distributions_dist",
        storage_set_key=StorageSetKey.METRICS,
        columns=ColumnSet(
            [
                *aggregated_columns,
                Column(
                    "percentiles",
                    AggregateFunction(
                        "quantiles(0.5, 0.75, 0.9, 0.95, 0.99, 1)", [Float(64)]
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
    query_processors=[ArrayJoinKeyValueOptimizer("tags")],
)
