from snuba.clickhouse.columns import UUID, Array, ColumnSet, DateTime, Nested
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.spans_processor import SpansMessageProcessor
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.type_converters.hexint_column_processor import (
    HexIntColumnProcessor,
)
from snuba.utils.streams.topics import Topic
from snuba.web.split import TimeSplitQueryStrategy

columns = ColumnSet(
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
    query_processors=[HexIntColumnProcessor({"transaction_span_id"})],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=SpansMessageProcessor(),
        default_topic=Topic.EVENTS,
        commit_log_topic=Topic.COMMIT_LOG,
    ),
    query_splitters=[TimeSplitQueryStrategy(timestamp_col="finish_ts")],
)
