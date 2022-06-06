"""
The storages defined in this file are for the generic metrics system,
initially built to handle metrics-enhanced performance.
"""
from snuba.clickhouse.columns import (
    AggregateFunction,
    Array,
    Column,
    ColumnSet,
    DateTime,
    Nested,
    SchemaModifiers,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.table_rate_limit import TableRateLimit

aggregated_columns = [
    Column("org_id", UInt(64)),
    Column("use_case_id", String()),
    Column("project_id", UInt(64)),
    Column("metric_id", UInt(64)),
    Column("granularity", UInt(8)),
    Column("timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column(
        "tags",
        Nested(
            [("key", UInt(64)), ("indexed_value", UInt(64)), ("raw_value", String())]
        ),
    ),
    Column("_raw_tags_hash", Array(UInt(64), SchemaModifiers(readonly=True))),
    Column("_indexed_tags_hash", Array(UInt(64), SchemaModifiers(readonly=True))),
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
                Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
            ]
        ),
    ),
    query_processors=[
        ArrayJoinKeyValueOptimizer("tags"),
        TableRateLimit(),
    ],
)
