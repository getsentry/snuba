from typing import List

from snuba.clickhouse.columns import (
    UUID,
    AggregateFunction,
    Array,
    Column,
    ColumnSet,
    DateTime,
    Float,
)
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, Tuple, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.functions_processor import FunctionsMessageProcessor
from snuba.datasets.schemas.tables import TableSchema, WritableTableSchema
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.table_rate_limit import TableRateLimit
from snuba.utils.schemas import SchemaModifiers
from snuba.utils.streams.topics import Topic

common_columns: List[Column[SchemaModifiers]] = [
    Column("project_id", UInt(64)),
    Column("transaction_name", String()),
    Column("timestamp", DateTime()),
    Column("depth", UInt(32)),
    Column("parent_fingerprint", UInt(64)),
    Column("fingerprint", UInt(64)),
    Column("name", String()),
    Column("package", String()),
    Column("path", String()),
    Column("is_application", UInt(8)),
    Column("platform", String()),
    Column("environment", String(Modifiers(nullable=True))),
    Column("release", String(Modifiers(nullable=True))),
    Column("os_name", String()),
    Column("os_version", String()),
    Column("retention_days", UInt(16)),
]

raw_storage = WritableTableStorage(
    storage_key=StorageKey.FUNCTIONS_RAW,
    storage_set_key=StorageSetKey.FUNCTIONS,
    schema=WritableTableSchema(
        storage_set_key=StorageSetKey.FUNCTIONS,
        local_table_name="functions_raw_local",
        dist_table_name="functions_raw_dist",
        columns=ColumnSet(
            common_columns
            + [
                Column("durations", Array(Float(64))),
                Column("profile_id", UUID()),
                Column("materialization_version", UInt(8)),
            ]
        ),
    ),
    query_processors=[],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=FunctionsMessageProcessor(),
        default_topic=Topic.PROFILES_FUNCTIONS,
    ),
)

agg_storage = ReadableTableStorage(
    storage_key=StorageKey.FUNCTIONS,
    storage_set_key=StorageSetKey.FUNCTIONS,
    schema=TableSchema(
        storage_set_key=StorageSetKey.FUNCTIONS,
        local_table_name="functions_mv_local",
        dist_table_name="functions_mv_dist",
        columns=ColumnSet(
            common_columns
            + [
                Column("count", AggregateFunction("count", [Float(64)])),
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
                Column(
                    "worst",
                    AggregateFunction("argMax", [Tuple((UUID(), UInt(64))), Float(64)]),
                ),
                Column(
                    "examples",
                    AggregateFunction(
                        "groupUniqArray(5)", [Tuple((UUID(), Float(64)))]
                    ),
                ),
            ]
        ),
    ),
    query_processors=[TableRateLimit()],
)
