from snuba import settings
from snuba.clickhouse.columns import (
    Array,
    ColumnSet,
    DateTime,
    Enum,
    Float,
    LowCardinality,
    Nested,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.querylog_processor import QuerylogProcessor
from snuba.datasets.schemas.tables import MergeTreeSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader


status_type = Enum([("success", 0), ("error", 1), ("rate-limited", 2)])

columns = ColumnSet(
    [
        ("request_id", UUID()),
        ("request_body", String()),
        ("referrer", LowCardinality(String())),
        ("dataset", LowCardinality(String())),
        ("projects", Array(UInt(64))),
        ("organization", Nullable(UInt(64))),
        ("timestamp", DateTime()),
        ("duration_ms", UInt(32)),
        ("status", status_type),
        (
            "clickhouse_queries",
            Nested(
                [
                    ("sql", String()),
                    ("status", status_type),
                    ("trace_id", Nullable(UUID())),
                    ("duration_ms", UInt(32)),
                    ("stats", String()),
                    ("final", UInt(8)),
                    ("cache_hit", UInt(8)),
                    ("sample", Float(32)),
                    ("max_threads", UInt(8)),
                    ("num_days", UInt(32)),
                    ("clickhouse_table", LowCardinality(String())),
                    ("query_id", String()),
                    # XXX: ``is_duplicate`` is currently not set when using the
                    # ``Cache.get_readthrough`` query execution path. See GH-902.
                    ("is_duplicate", UInt(8)),
                    ("consistent", UInt(8)),
                ]
            ),
        ),
    ]
)

schema = MergeTreeSchema(
    columns=columns,
    local_table_name="querylog_local",
    dist_table_name="querylog_dist",
    order_by="(toStartOfDay(timestamp), request_id)",
    partition_by="(toMonday(timestamp))",
    sample_expr="request_id",
)

storage = WritableTableStorage(
    storage_key=StorageKey.QUERYLOG,
    storage_set_key=StorageSetKey.QUERYLOG,
    schemas=StorageSchemas(read_schema=schema, write_schema=schema),
    query_processors=[],
    stream_loader=KafkaStreamLoader(
        processor=QuerylogProcessor(), default_topic=settings.QUERIES_TOPIC,
    ),
)
