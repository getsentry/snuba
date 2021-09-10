from snuba.clickhouse.columns import UUID, ColumnSet, DateTime, Float
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.spans_processor import SpansMessageProcessorV2
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
        ("span_group_id", UUID()),
        ("op", String()),
        ("status", UInt(8)),
        # transaction start/finish times
        ("start_ts", DateTime()),
        ("start_ns", UInt(32)),
        ("finish_ts", DateTime()),
        ("finish_ns", UInt(32)),
        # span start/finish times
        ("span_start_ts", DateTime()),
        ("span_start_ns", UInt(32)),
        ("span_finish_ts", DateTime()),
        ("span_finish_ns", UInt(32)),
        ("duration_ms", UInt(32)),
        ("exclusive_time_ms", Float(64)),
        ("retention_days", UInt(16)),
        ("deleted", UInt(8)),
    ]
)

schema = WritableTableSchema(
    columns=columns,
    local_table_name="spans_v2_local",
    dist_table_name="spans_v2_dist",
    storage_set_key=StorageSetKey.TRANSACTIONS,
)

storage = WritableTableStorage(
    storage_key=StorageKey.SPANS_V2,
    storage_set_key=StorageSetKey.TRANSACTIONS,
    schema=schema,
    query_processors=[HexIntColumnProcessor({"transaction_span_id"})],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=SpansMessageProcessorV2(),
        default_topic=Topic.EVENTS,
        commit_log_topic=Topic.COMMIT_LOG,
    ),
    query_splitters=[TimeSplitQueryStrategy(timestamp_col="finish_ts")],
)
