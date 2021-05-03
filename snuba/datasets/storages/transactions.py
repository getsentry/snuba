from snuba import util
from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    DateTime,
    Float,
    IPv4,
    IPv6,
    Nested,
)
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.events_bool_contexts import EventsBooleanContextsProcessor
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.datasets.transactions_processor import TransactionsMessageProcessor
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.mapping_optimizer import MappingOptimizer
from snuba.query.processors.mapping_promoter import MappingColumnPromoter
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.processors.type_converters.uuid_column_processor import (
    UUIDColumnProcessor,
)
from snuba.query.processors.type_converters.hexint_column_processor import (
    HexIntColumnProcessor,
)
from snuba.utils.streams.topics import Topic
from snuba.web.split import TimeSplitQueryStrategy

columns = ColumnSet(
    [
        ("project_id", UInt(64)),
        ("event_id", UUID()),
        ("trace_id", UUID(Modifiers(nullable=True))),
        ("span_id", UInt(64)),
        ("transaction_name", String()),
        ("transaction_hash", UInt(64, Modifiers(readonly=True))),
        ("transaction_op", String()),
        ("transaction_status", UInt(8)),
        ("start_ts", DateTime()),
        ("start_ms", UInt(16)),
        ("finish_ts", DateTime()),
        ("finish_ms", UInt(16)),
        ("duration", UInt(32)),
        ("platform", String()),
        ("environment", String(Modifiers(nullable=True))),
        ("release", String(Modifiers(nullable=True))),
        ("dist", String(Modifiers(nullable=True))),
        ("ip_address_v4", IPv4(Modifiers(nullable=True))),
        ("ip_address_v6", IPv6(Modifiers(nullable=True))),
        ("user", String()),
        ("user_hash", UInt(64, Modifiers(readonly=True))),
        ("user_id", String(Modifiers(nullable=True))),
        ("user_name", String(Modifiers(nullable=True))),
        ("user_email", String(Modifiers(nullable=True))),
        ("sdk_name", String()),
        ("sdk_version", String()),
        ("http_method", String(Modifiers(nullable=True))),
        ("http_referer", String(Modifiers(nullable=True))),
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_flattened", String()),
        ("_tags_hash_map", Array(UInt(64), Modifiers(readonly=True))),
        ("contexts", Nested([("key", String()), ("value", String())])),
        ("_contexts_flattened", String()),
        ("measurements", Nested([("key", String()), ("value", Float(64))]),),
        ("span_op_breakdowns", Nested([("key", String()), ("value", Float(64))]),),
        ("partition", UInt(16)),
        ("offset", UInt(64)),
        ("message_timestamp", DateTime()),
        ("retention_days", UInt(16)),
        ("deleted", UInt(8)),
        ("type", String(Modifiers(readonly=True))),
        ("message", String(Modifiers(readonly=True))),
        ("title", String(Modifiers(readonly=True))),
        ("timestamp", DateTime(Modifiers(readonly=True))),
    ]
)

schema = WritableTableSchema(
    columns=columns,
    local_table_name="transactions_local",
    dist_table_name="transactions_dist",
    storage_set_key=StorageSetKey.TRANSACTIONS,
    mandatory_conditions=[],
    part_format=[util.PartSegment.RETENTION_DAYS, util.PartSegment.DATE],
)


storage = WritableTableStorage(
    storage_key=StorageKey.TRANSACTIONS,
    storage_set_key=StorageSetKey.TRANSACTIONS,
    schema=schema,
    query_processors=[
        MappingColumnPromoter(
            mapping_specs={
                "tags": {
                    "environment": "environment",
                    "sentry:release": "release",
                    "sentry:dist": "dist",
                    "sentry:user": "user",
                },
                "contexts": {"trace.trace_id": "trace_id", "trace.span_id": "span_id"},
            }
        ),
        UUIDColumnProcessor(set(["event_id", "trace_id"])),
        HexIntColumnProcessor({"span_id"}),
        EventsBooleanContextsProcessor(),
        MappingOptimizer("tags", "_tags_hash_map", "tags_hash_map_enabled"),
        ArrayJoinKeyValueOptimizer("tags"),
        ArrayJoinKeyValueOptimizer("measurements"),
        ArrayJoinKeyValueOptimizer("span_op_breakdowns"),
        PrewhereProcessor(
            [
                "event_id",
                "trace_id",
                "span_id",
                "transaction_name",
                "transaction",
                "title",
            ]
        ),
    ],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=TransactionsMessageProcessor(),
        default_topic=Topic.EVENTS,
        commit_log_topic=Topic.COMMIT_LOG,
    ),
    query_splitters=[TimeSplitQueryStrategy(timestamp_col="finish_ts")],
    writer_options={"insert_allow_materialized_columns": 1},
)
