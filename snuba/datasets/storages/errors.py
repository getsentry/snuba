from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    DateTime,
    IPv4,
    IPv6,
    Nested,
    String,
    UInt,
    nullable,
    readonly,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.errors_processor import ErrorsProcessor
from snuba.datasets.errors_replacer import ErrorsReplacer, ReplacerState
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.processors.replaced_groups import (
    PostReplacementConsistencyEnforcer,
)
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.expressions import Column, Literal
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.mapping_promoter import MappingColumnPromoter
from snuba.query.processors.prewhere import PrewhereProcessor

all_columns = ColumnSet(
    [
        ("project_id", UInt(64)),
        ("timestamp", DateTime()),
        ("event_id", UUID()),
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
        ("sdk_name", String(nullable())),
        ("sdk_version", String(nullable())),
        ("http_method", String(nullable())),
        ("http_referer", String(nullable())),
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_hash_map", Array(UInt(64), readonly())),
        ("contexts", Nested([("key", String()), ("value", String())])),
        ("transaction_name", String()),
        ("transaction_hash", UInt(64, readonly())),
        ("span_id", UInt(64, nullable())),
        ("trace_id", UUID(nullable())),
        ("partition", UInt(16)),
        ("offset", UInt(64)),
        ("message_timestamp", DateTime()),
        ("retention_days", UInt(16)),
        ("deleted", UInt(8)),
        ("group_id", UInt(64)),
        ("primary_hash", UUID()),
        ("received", DateTime()),
        ("message", String()),
        ("title", String()),
        ("culprit", String()),
        ("level", String()),
        ("location", String(nullable())),
        ("version", String(nullable())),
        ("type", String()),
        (
            "exception_stacks",
            Nested(
                [
                    ("type", String(nullable())),
                    ("value", String(nullable())),
                    ("mechanism_type", String(nullable())),
                    ("mechanism_handled", UInt(8, nullable())),
                ]
            ),
        ),
        (
            "exception_frames",
            Nested(
                [
                    ("abs_path", String(nullable())),
                    ("colno", UInt(32, nullable())),
                    ("filename", String(nullable())),
                    ("function", String(nullable())),
                    ("lineno", UInt(32, nullable())),
                    ("in_app", UInt(8, nullable())),
                    ("package", String(nullable())),
                    ("module", String(nullable())),
                    ("stack_level", UInt(16, nullable())),
                ]
            ),
        ),
        ("sdk_integrations", Array(String())),
        ("modules", Nested([("name", String()), ("version", String())])),
    ]
)

promoted_tag_columns = {
    "environment": "environment",
    "sentry:release": "release",
    "sentry:dist": "dist",
    "sentry:user": "user",
    "transaction": "transaction_name",
    "level": "level",
}

schema = WritableTableSchema(
    columns=all_columns,
    local_table_name="errors_local",
    dist_table_name="errors_dist",
    storage_set_key=StorageSetKey.EVENTS,
    mandatory_conditions=[
        binary_condition(
            None,
            ConditionFunctions.EQ,
            Column(None, None, "deleted"),
            Literal(None, 0),
        ),
    ],
    prewhere_candidates=[
        "event_id",
        "group_id",
        "tags[sentry:release]",
        "message",
        "environment",
        "project_id",
    ],
)

required_columns = [
    "event_id",
    "project_id",
    "group_id",
    "timestamp",
    "deleted",
    "retention_days",
]

storage = WritableTableStorage(
    storage_key=StorageKey.ERRORS,
    storage_set_key=StorageSetKey.EVENTS,
    schema=schema,
    query_processors=[
        PostReplacementConsistencyEnforcer(
            project_column="project_id", replacer_state_name=ReplacerState.ERRORS,
        ),
        MappingColumnPromoter(mapping_specs={"tags": promoted_tag_columns}),
        ArrayJoinKeyValueOptimizer("tags"),
        PrewhereProcessor(),
    ],
    stream_loader=KafkaStreamLoader(
        processor=ErrorsProcessor(promoted_tag_columns),
        default_topic="events",
        replacement_topic="errors-replacements",
    ),
    replacer_processor=ErrorsReplacer(
        schema=schema,
        required_columns=required_columns,
        tag_column_map={"tags": promoted_tag_columns, "contexts": {}},
        promoted_tags={"tags": list(promoted_tag_columns.keys()), "contexts": []},
        state_name=ReplacerState.ERRORS,
        use_promoted_prewhere=False,
    ),
)
