from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    DateTime,
    Float,
    IPv4,
    IPv6,
    Nested,
    nullable,
    readonly,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.transaction_column_processor import (
    TransactionColumnProcessor,
)
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.datasets.transactions_processor import TransactionsMessageProcessor
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.processors.mapping_optimizer import MappingOptimizer
from snuba.web.split import TimeSplitQueryStrategy


columns = ColumnSet(
    [
        ("project_id", UInt(64)),
        ("event_id", UUID()),
        ("trace_id", UUID()),
        ("span_id", UInt(64)),
        ("transaction_name", String()),
        ("transaction_hash", UInt(64, readonly())),
        ("transaction_op", String()),
        ("transaction_status", UInt(8)),
        ("start_ts", DateTime()),
        ("start_ms", UInt(16)),
        ("finish_ts", DateTime()),
        ("finish_ms", UInt(16)),
        ("duration", UInt(32)),
        ("platform", String()),
        ("environment", String(nullable())),
        ("release", String(nullable())),
        ("dist", String(nullable())),
        ("ip_address_v4", IPv4(nullable())),
        ("ip_address_v6", IPv6(nullable())),
        ("user", String()),
        ("user_hash", UInt(64, readonly())),
        ("user_id", String(nullable())),
        ("user_name", String(nullable())),
        ("user_email", String(nullable())),
        ("sdk_name", String()),
        ("sdk_version", String()),
        ("http_method", String(nullable())),
        ("http_referer", String(nullable())),
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_flattened", String()),
        ("_tags_hash_map", Array(UInt(64), readonly())),
        ("contexts", Nested([("key", String()), ("value", String())])),
        ("_contexts_flattened", String()),
        ("measurements", Nested([("key", String()), ("value", Float(64))]),),
        ("partition", UInt(16)),
        ("offset", UInt(64)),
        ("message_timestamp", DateTime()),
        ("retention_days", UInt(16)),
        ("deleted", UInt(8)),
    ]
)

schema = WritableTableSchema(
    columns=columns,
    local_table_name="transactions_local",
    dist_table_name="transactions_dist",
    storage_set_key=StorageSetKey.TRANSACTIONS,
    mandatory_conditions=[],
    prewhere_candidates=["event_id", "transaction_name", "transaction", "title"],
)


storage = WritableTableStorage(
    storage_key=StorageKey.TRANSACTIONS,
    storage_set_key=StorageSetKey.TRANSACTIONS,
    schema=schema,
    query_processors=[
        MappingOptimizer("tags", "_tags_hash_map", "tags_hash_map_enabled"),
        TransactionColumnProcessor(),
        ArrayJoinKeyValueOptimizer("tags"),
        ArrayJoinKeyValueOptimizer("measurements"),
        PrewhereProcessor(),
    ],
    stream_loader=KafkaStreamLoader(
        processor=TransactionsMessageProcessor(), default_topic="events",
    ),
    query_splitters=[TimeSplitQueryStrategy(timestamp_col="finish_ts")],
    writer_options={"insert_allow_materialized_columns": 1},
)
