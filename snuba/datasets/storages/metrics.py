from snuba.datasets.metrics_processor import MetricsProcessor
from snuba.clickhouse.columns import (
    AggregateFunction,
    Array,
    Column,
    ColumnSet,
    DateTime,
    Float,
    Nested,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings

buckets_columns = ColumnSet(
    [
        Column("org_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("metric_id", UInt(64)),
        Column("metric_type", String()),
        Column("timestamp", DateTime()),
        Column("tags", Nested([Column("key", UInt(64)), Column("value", UInt(64))])),
        Column("sum_values", Float(64)),
        Column("distribution_values", Array(Float(64))),
        Column("set_values", Array(UInt(64))),
        Column("materialization_version", UInt(8)),
        Column("retention_days", UInt(16)),
        Column("partition", UInt(16)),
        Column("offset", UInt(64)),
    ]
)

buckets_storage = WritableTableStorage(
    storage_key=StorageKey.METRICS_BUCKETS,
    storage_set_key=StorageSetKey.METRICS,
    schema=WritableTableSchema(
        columns=buckets_columns,
        # TODO: change to outcomes.raw_local when we add multi DB support
        local_table_name="metrics_buckets_local",
        dist_table_name="metrics_buckets_dist",
        storage_set_key=StorageSetKey.METRICS,
    ),
    query_processors=[],
    stream_loader=build_kafka_stream_loader_from_settings(
        StorageKey.METRICS_BUCKETS,
        processor=MetricsProcessor(),
        default_topic_name="metrics",
    ),
)

sets_columns = ColumnSet(
    [
        Column("org_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("metric_id", UInt(64)),
        Column("granularity", UInt(32)),
        Column("timestamp", DateTime()),
        Column("retention_days", UInt(16)),
        Column("tags.key", Array(UInt(64))),
        Column("tags.value", Array(UInt(64))),
        Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
    ]
)

sets_storage = ReadableTableStorage(
    storage_key=StorageKey.METRICS_SETS,
    storage_set_key=StorageSetKey.METRICS,
    schema=TableSchema(
        local_table_name="metrics_sets_local",
        dist_table_name="metrics_sets_dist",
        storage_set_key=StorageSetKey.METRICS,
        columns=sets_columns,
    ),
    query_processors=[],
)
