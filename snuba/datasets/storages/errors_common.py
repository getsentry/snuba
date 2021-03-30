from snuba.clickhouse.columns import (
    UUID,
    Array,
    ColumnSet,
    DateTime,
    IPv4,
    IPv6,
    Nested,
)
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.datasets.errors_replacer import ReplacerState
from snuba.datasets.storages.events_bool_contexts import EventsBooleanContextsProcessor
from snuba.datasets.storages.processors.replaced_groups import (
    PostReplacementConsistencyEnforcer,
)
from snuba.datasets.storages.type_condition_optimizer import TypeConditionOptimizer
from snuba.datasets.storages.user_column_processor import UserColumnProcessor
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.expressions import Column, Literal
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.mapping_optimizer import MappingOptimizer
from snuba.query.processors.mapping_promoter import MappingColumnPromoter
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.processors.uuid_column_processor import UUIDColumnProcessor
from snuba.web.split import ColumnSplitQueryStrategy, TimeSplitQueryStrategy

required_columns = [
    "event_id",
    "primary_hash",
    "project_id",
    "group_id",
    "timestamp",
    "deleted",
    "retention_days",
]

all_columns = ColumnSet(
    [
        ("project_id", UInt(64)),
        ("timestamp", DateTime()),
        ("event_id", UUID()),
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
        ("sdk_name", String(Modifiers(nullable=True))),
        ("sdk_version", String(Modifiers(nullable=True))),
        ("http_method", String(Modifiers(nullable=True))),
        ("http_referer", String(Modifiers(nullable=True))),
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_hash_map", Array(UInt(64), Modifiers(readonly=True))),
        ("contexts", Nested([("key", String()), ("value", String())])),
        ("transaction_name", String()),
        ("transaction_hash", UInt(64, Modifiers(readonly=True))),
        ("span_id", UInt(64, Modifiers(nullable=True))),
        ("trace_id", UUID(Modifiers(nullable=True))),
        ("partition", UInt(16)),
        ("offset", UInt(64)),
        ("message_timestamp", DateTime()),
        ("retention_days", UInt(16)),
        ("deleted", UInt(8)),
        ("group_id", UInt(64)),
        ("primary_hash", UUID()),
        ("hierarchical_hashes", Array(UUID())),
        ("received", DateTime()),
        ("message", String()),
        ("title", String()),
        ("culprit", String()),
        ("level", String(Modifiers(nullable=True))),
        ("location", String(Modifiers(nullable=True))),
        ("version", String(Modifiers(nullable=True))),
        ("type", String()),
        (
            "exception_stacks",
            Nested(
                [
                    ("type", String(Modifiers(nullable=True))),
                    ("value", String(Modifiers(nullable=True))),
                    ("mechanism_type", String(Modifiers(nullable=True))),
                    ("mechanism_handled", UInt(8, Modifiers(nullable=True))),
                ]
            ),
        ),
        (
            "exception_frames",
            Nested(
                [
                    ("abs_path", String(Modifiers(nullable=True))),
                    ("colno", UInt(32, Modifiers(nullable=True))),
                    ("filename", String(Modifiers(nullable=True))),
                    ("function", String(Modifiers(nullable=True))),
                    ("lineno", UInt(32, Modifiers(nullable=True))),
                    ("in_app", UInt(8, Modifiers(nullable=True))),
                    ("package", String(Modifiers(nullable=True))),
                    ("module", String(Modifiers(nullable=True))),
                    ("stack_level", UInt(16, Modifiers(nullable=True))),
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

promoted_context_columns = {"trace.trace_id": "trace_id"}

mandatory_conditions = [
    binary_condition(
        ConditionFunctions.EQ, Column(None, None, "deleted"), Literal(None, 0),
    ),
]

prewhere_candidates = [
    "event_id",
    "trace_id",
    "group_id",
    "tags[sentry:release]",
    "release",
    "message",
    "environment",
    "project_id",
]

query_processors = [
    PostReplacementConsistencyEnforcer(
        project_column="project_id", replacer_state_name=ReplacerState.ERRORS,
    ),
    MappingColumnPromoter(
        mapping_specs={
            "tags": promoted_tag_columns,
            "contexts": promoted_context_columns,
        }
    ),
    UserColumnProcessor(),
    UUIDColumnProcessor({"event_id", "primary_hash", "trace_id"}),
    EventsBooleanContextsProcessor(),
    TypeConditionOptimizer(),
    MappingOptimizer("tags", "_tags_hash_map", "events_tags_hash_map_enabled"),
    ArrayJoinKeyValueOptimizer("tags"),
    PrewhereProcessor(prewhere_candidates, omit_if_final=["environment", "release"]),
]

query_splitters = [
    ColumnSplitQueryStrategy(
        id_column="event_id", project_column="project_id", timestamp_column="timestamp",
    ),
    TimeSplitQueryStrategy(timestamp_col="timestamp"),
]
