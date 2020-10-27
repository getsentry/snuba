from snuba.clickhouse.columns import UUID, Array, ColumnSet, DateTime, Nested
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.spans_processor import SpansMessageProcessor
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.web.split import TimeSplitQueryStrategy

columns = ColumnSet[Modifiers](
    [
        ("project_id", UInt(64)),
        ("transaction_id", UUID()),
        ("trace_id", UUID()),
        ("transaction_span_id", UInt(64)),
        ("span_id", UInt(64)),
        ("parent_span_id", UInt(64, Modifiers(nullable=True))),
        ("transaction_name", String()),
        ("description", String()),  # description in span
        ("op", String()),
        ("status", UInt(8)),
        ("start_ts", DateTime()),
        ("start_ns", UInt(32)),
        ("finish_ts", DateTime()),
        ("finish_ns", UInt(32)),
        ("duration_ms", UInt(32)),
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_hash_map", Array(UInt(64), Modifiers(readonly=True))),
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
