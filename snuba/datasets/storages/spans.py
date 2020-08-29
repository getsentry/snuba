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
        ("trace_id", UUID()),
        ("span_type", LowCardinality(String())),  # span/transaction
        ("span_id", UInt(64)),
        ("parent_span_id", UInt(64)),  # Is this actually the transaction id ?
        ("name", LowCardinality(String())),  # description in span
        ("name_hash", Materialized(UInt(64), "cityHash64(name)",),),
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
        # These are transaction specific
        ("platform", LowCardinality(Nullable(String()))),
        ("environment", LowCardinality(Nullable(String()))),
        ("release", LowCardinality(Nullable(String()))),
        ("dist", LowCardinality(Nullable(String()))),
        ("ip_address_v4", Nullable(IPv4())),
        ("ip_address_v6", Nullable(IPv6())),
        ("user", WithDefault(String(), "''",)),
        ("user_hash", Materialized(UInt(64), "cityHash64(user)"),),
        ("user_id", Nullable(String())),
        ("user_name", Nullable(String())),
        ("user_email", Nullable(String())),
        ("sdk_name", WithDefault(LowCardinality(String()), "''")),
        ("sdk_version", WithDefault(LowCardinality(String()), "''")),
        ("http_method", LowCardinality(Nullable(String()))),
        ("http_referer", Nullable(String())),
        # back to spans
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_hash_map", Materialized(Array(UInt(64)), TAGS_HASH_MAP_COLUMN)),
        ("contexts", Nested([("key", String()), ("value", String())])),
        (
            "measurements",
            Nested([("key", LowCardinality(String())), ("value", Float(64))]),
        ),
        ("partition", UInt(16)),
        ("offset", UInt(64)),
        ("message_timestamp", DateTime()),
        ("retention_days", UInt(16)),
        ("deleted", UInt(8)),
    ]
)

schema = ReplacingMergeTreeSchema(
    columns=columns,
    local_table_name="spans_local",
    dist_table_name="spans_dist",
    storage_set_key=StorageSetKey.TRANSACTIONS,
    mandatory_conditions=[],
    prewhere_candidates=["transaction_id", "name", "transaction", "title"],
    order_by="(project_id, toStartOfDay(finish_ts), span_type, name, cityHash64(parent_span_id), cityHash64(span_id))",
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
