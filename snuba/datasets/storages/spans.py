from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    DateTime,
    LowCardinality,
    Materialized,
    Nested,
    String,
    UInt,
    WithDefault,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.spans_processor import UNKNOWN_SPAN_STATUS, SpansMessageProcessor
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.web.split import TimeSplitQueryStrategy

columns = ColumnSet(
    [
        ("project_id", UInt(64)),
        ("trace_id", UUID()),
        ("transaction_span_id", UInt(64)),
        ("span_id", UInt(64)),
        ("parent_span_id", UInt(64)),
        ("transaction_name", LowCardinality(String())),
        ("description", LowCardinality(String())),  # description in span
        ("op", LowCardinality(String())),
        ("status", WithDefault(UInt(8), str(UNKNOWN_SPAN_STATUS)),),
        ("start_ts", DateTime()),
        ("start_ns", UInt(32)),
        ("finish_ts", DateTime()),
        ("finish_ns", UInt(32)),
        ("duration", UInt(32)),
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_hash_map", Materialized(Array(UInt(64)), TAGS_HASH_MAP_COLUMN)),
        ("retention_days", UInt(16)),
        ("deleted", UInt(8)),
    ]
)

schema = WritableTableSchema(
    columns=columns,
    local_table_name="spans_experimental_local",
    dist_table_name="spans_experimental_dist",
    storage_set_key=StorageSetKey.TRANSACTIONS,
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
)
