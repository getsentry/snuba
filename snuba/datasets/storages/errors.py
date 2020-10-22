from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    DateTime,
    IPv4,
    IPv6,
    Nested,
    Nullable,
    ReadOnly,
    String,
    UInt,
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
        ("environment", String([Nullable()])),
        ("release", String([Nullable()])),
        ("dist", String([Nullable()])),
        ("ip_address_v4", IPv4([Nullable()])),
        ("ip_address_v6", IPv6([Nullable()])),
        ("user", String()),
        ("user_hash", UInt(64, [ReadOnly()])),
        ("user_id", String([Nullable()])),
        ("user_name", String([Nullable()])),
        ("user_email", String([Nullable()])),
        ("sdk_name", String([Nullable()])),
        ("sdk_version", String([Nullable()])),
        ("http_method", String([Nullable()])),
        ("http_referer", String([Nullable()])),
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_hash_map", Array(UInt(64), [ReadOnly()])),
        ("contexts", Nested([("key", String()), ("value", String())])),
        ("transaction_name", String()),
        ("transaction_hash", UInt(64, [ReadOnly()])),
        ("span_id", UInt(64, [Nullable()])),
        ("trace_id", UUID([Nullable()])),
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
        ("location", String([Nullable()])),
        ("version", String([Nullable()])),
        ("type", String()),
        (
            "exception_stacks",
            Nested(
                [
                    ("type", String([Nullable()])),
                    ("value", String([Nullable()])),
                    ("mechanism_type", String([Nullable()])),
                    ("mechanism_handled", UInt(8, [Nullable()])),
                ]
            ),
        ),
        (
            "exception_frames",
            Nested(
                [
                    ("abs_path", String([Nullable()])),
                    ("colno", UInt(32, [Nullable()])),
                    ("filename", String([Nullable()])),
                    ("function", String([Nullable()])),
                    ("lineno", UInt(32, [Nullable()])),
                    ("in_app", UInt(8, [Nullable()])),
                    ("package", String([Nullable()])),
                    ("module", String([Nullable()])),
                    ("stack_level", UInt(16, [Nullable()])),
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
    ),
)
