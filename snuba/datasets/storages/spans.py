from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    DateTime,
    Float,
    IPv4,
    IPv6,
    LowCardinality,
    Materialized,
    Nested,
    Nullable,
    String,
    UInt,
    WithDefault,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.datasets.spans_processor import (
    UNKNOWN_SPAN_STATUS,
    SpansMessageProcessor,
)
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.web.split import TimeSplitQueryStrategy


columns = ColumnSet(
    [
        ("project_id", UInt(64)),
        ("transaction_id", UUID()),  # event_id of the transaction
        ("span_id", UInt(64)),
        ("parent_span_id", UInt(64)),  # Is this actually the transaction id ?
        ("description", LowCardinality(String())),  # description in span
        ("op", LowCardinality(String())),
        (
            "status",
            WithDefault(UInt(8), str(UNKNOWN_SPAN_STATUS)),
        ),  # This was transaction_status
        ("start_ts", DateTime()),
        ("start_ms", UInt(16)),
        ("finish_ts", DateTime()),
        ("finish_ms", UInt(16)),
        ("duration", UInt(32)),
        # back to spans
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_hash_map", Materialized(Array(UInt(64)), TAGS_HASH_MAP_COLUMN)),
        ("retention_days", UInt(16)),
        ("deleted", UInt(8)),
    ]
)

schema = ReplacingMergeTreeSchema(
    columns=columns,
    local_table_name="single_spans_local",
    dist_table_name="single_spans_dist",
    storage_set_key=StorageSetKey.TRANSACTIONS,
    mandatory_conditions=[],
    prewhere_candidates=["transaction_id", "description"],
    order_by="(project_id, toStartOfDay(finish_ts), cityHash64(parent_span_id), cityHash64(span_id))",
    partition_by="(retention_days, toMonday(finish_ts))",
    version_column="deleted",
    sample_expr="cityHash64(span_id)",
    ttl_expr="finish_ts + toIntervalDay(retention_days)",
    settings={"index_granularity": "8192"},
    # Tags hashmap is a materialized column. Clickhouse does not allow
    # us to create a materialized column that references a nested one
    # during create statement
    # (https://github.com/ClickHouse/ClickHouse/issues/12586), so the
    # materialization is added with a migration.
    skipped_cols_on_creation={"_tags_hash_map"},
)


storage = WritableTableStorage(
    storage_key=StorageKey.SPANS,
    storage_set_key=StorageSetKey.TRANSACTIONS,
    schema=schema,
    query_processors=[PrewhereProcessor()],
    stream_loader=KafkaStreamLoader(
        processor=SpansMessageProcessor(), default_topic="events",
    ),
    query_splitters=[TimeSplitQueryStrategy(timestamp_col="finish_ts")],
    writer_options={"insert_allow_materialized_columns": 1},
)
